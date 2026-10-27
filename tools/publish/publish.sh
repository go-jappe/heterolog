#!/usr/bin/env bash
# publish.shは、このrepositoryのcommitとtagを、公開用の名前とGitHub Appのtokenだけを使って
# GitHubのrepositoryへ送ります。送る前に、作者と日時、header、message、file名と内容を検査します。
# 準備と使い方はtools/publish/README.mdを参照してください。
#
# macOSのbash 3.2でも動くよう、bash 4以降の機能（連想配列、mapfileなど）と、set -uの下で空の
# 配列を展開する書き方は使いません。外部commandも、macOS・Linux・Git for Windowsに共通する
# optionだけを使います。
set -euo pipefail

usage() {
	cat <<'USAGE'
Usage: tools/publish/publish.sh COMMAND [--dry-run] [VERSION]

Commands:
  setup            configure this clone to commit as the publisher and to
                   refuse plain git pushes to the repository
  fetch            fetch the branches and tags of the repository
  push             check the commits of local main that the repository does
                   not have yet, and push them to its main
  fix              rewrite those commits with the publisher identity and UTC
                   dates, keeping their content, messages and times
  release VERSION  tag the pushed main as VERSION (for example v0.5.0) and
                   push the tag

push, fix and release accept --dry-run, which only checks and prints what
would be pushed, rewritten or tagged. Every command except setup connects to
GitHub with a GitHub App installation token.
USAGE
}

die() {
	echo "publish: $*" >&2
	exit 1
}

warn() {
	echo "publish: warning: $*" >&2
}

[[ $# -gt 0 ]] || {
	usage >&2
	exit 2
}
command=$1
shift
case $command in
-h | --help)
	usage
	exit 0
	;;
esac
dry_run=false
version=""
for arg in "$@"; do
	case $arg in
	--dry-run) dry_run=true ;;
	-h | --help)
		usage
		exit 0
		;;
	-*) die "unknown option $arg" ;;
	*)
		[[ $command == release && -z $version ]] || die "unexpected argument $arg"
		version=$arg
		;;
	esac
done
version_pattern='^v[0-9]+[.][0-9]+[.][0-9]+(-[0-9A-Za-z.-]+)?$'
case $command in
setup | fetch) ! $dry_run || die "$command has no --dry-run" ;;
push | fix) ;;
release)
	[[ -n $version ]] || die "release needs VERSION"
	[[ $version =~ $version_pattern ]] || die "VERSION must look like v1.2.3: $version"
	;;
*)
	usage >&2
	exit 2
	;;
esac

# hookやaliasから実行した場合に引き継がれるgitの環境変数は、別のrepositoryや設定、作者を指す
# ことがあるため外します。作者と日時は、このscriptが必要な箇所でだけ指定します。
unset GIT_DIR GIT_WORK_TREE GIT_COMMON_DIR GIT_INDEX_FILE GIT_OBJECT_DIRECTORY \
	GIT_ALTERNATE_OBJECT_DIRECTORIES GIT_NAMESPACE GIT_CONFIG GIT_CONFIG_PARAMETERS GIT_CONFIG_COUNT \
	GIT_AUTHOR_NAME GIT_AUTHOR_EMAIL GIT_AUTHOR_DATE GIT_COMMITTER_NAME GIT_COMMITTER_EMAIL GIT_COMMITTER_DATE

required_commands="git curl jq openssl"
[[ $command != setup ]] || required_commands=git
for required_command in $required_commands; do
	command -v "$required_command" >/dev/null || die "$required_command is required"
done

config_dir=${HETEROLOG_PUBLISH_CONFIG_DIR:-$HOME/.config/heterolog-publish}
config_file=$config_dir/config
app_id="" app_slug="" app_key="" public_repo="" publish_name="" bot_user_id=""
[[ -r $config_file ]] || die "missing $config_file (see tools/publish/README.md)"
# 設定fileは実行せず、既知のkeyだけを読みます。
while IFS='=' read -r key value || [[ -n $key ]]; do
	key=${key%$'\r'}
	value=${value%$'\r'}
	[[ -z $key || $key == \#* ]] && continue
	case $key in
	APP_ID) app_id=$value ;;
	APP_SLUG) app_slug=$value ;;
	APP_KEY) app_key=${value/#\~/$HOME} ;;
	PUBLIC_REPO) public_repo=$value ;;
	PUBLISH_NAME) publish_name=$value ;;
	BOT_USER_ID) bot_user_id=$value ;;
	*) die "unknown key $key in $config_file" ;;
	esac
done <"$config_file"
for required in APP_ID APP_SLUG APP_KEY PUBLIC_REPO PUBLISH_NAME BOT_USER_ID; do
	variable=$(printf '%s' "$required" | tr '[:upper:]' '[:lower:]')
	[[ -n ${!variable} ]] || die "$required is not set in $config_file"
done
repo_pattern='^[A-Za-z0-9-]+/[A-Za-z0-9._-]+$'
[[ $public_repo =~ $repo_pattern ]] || die "PUBLIC_REPO must look like owner/name: $public_repo"
slug_pattern='^[a-z0-9-]+$'
number_pattern='^[0-9]+$'
[[ $app_slug =~ $slug_pattern && $bot_user_id =~ $number_pattern && $app_id =~ $number_pattern ]] ||
	die "APP_ID and BOT_USER_ID must be numbers and APP_SLUG a slug in $config_file"

bot_login="$app_slug[bot]"
bot_email="$bot_user_id+$bot_login@users.noreply.github.com"
identity="$publish_name <$bot_email>"
remote_url="https://github.com/$public_repo.git"
# setupがoriginのpush先に設定する値です。存在しないlocalのpathとして扱われ、git pushは失敗します。
no_push_url="push-with-tools/publish/publish.sh"
hook_marker="heterolog-publish pre-push hook"

repo_root=$(git rev-parse --show-toplevel 2>/dev/null) || die "run this inside a clone of $public_repo"
cd "$repo_root"

work=$(mktemp -d "${TMPDIR:-/tmp}/heterolog-publish.XXXXXX")
trap 'rm -rf "$work"' EXIT
mkdir "$work/home"
auth_header=""
token=""

# GitHubへのgitは、利用者のgit設定（認証情報helper、URLの書き換え、hookなど）・.netrc・promptを
# 使わず、このscriptが取得したtokenだけで接続します。個人の認証情報で書き込まないためです。
# 作業用のrepositoryで実行すると、そのrepositoryの設定だけが加わります。
isolated_git() {
	HOME=$work/home XDG_CONFIG_HOME=$work/home/.config \
		GIT_CONFIG_NOSYSTEM=1 GIT_CONFIG_GLOBAL=/dev/null GIT_TERMINAL_PROMPT=0 \
		GIT_ASKPASS=/bin/false SSH_ASKPASS=/bin/false \
		GIT_CONFIG_COUNT=2 GIT_CONFIG_KEY_0=credential.helper GIT_CONFIG_VALUE_0= \
		GIT_CONFIG_KEY_1=http.https://github.com/.extraheader GIT_CONFIG_VALUE_1="$auth_header" \
		git "$@"
}

api() {
	local method=$1 path=$2 authorization=$3
	shift 3
	# 認証headerはprocessの引数に出さないよう、標準入力の設定として渡します。
	curl -q -fsS -X "$method" --config - \
		-H "Accept: application/vnd.github+json" -H "X-GitHub-Api-Version: 2022-11-28" \
		"$@" "https://api.github.com$path" <<<"header = \"Authorization: $authorization\""
}

base64url() { openssl base64 -A | tr '+/' '-_' | tr -d '='; }

# get_tokenは、GitHub Appの秘密鍵でJWTを作り、公開用repositoryだけに使えるinstallation tokenを
# 取得します。permissionsは、tokenに付けるrepositoryの権限（JSON）です。
get_token() {
	local permissions=$1 now header payload signature jwt installation_id
	[[ -r $app_key ]] || die "cannot read the GitHub App private key $app_key"
	now=$(date +%s)
	header=$(printf '{"alg":"RS256","typ":"JWT"}' | base64url)
	payload=$(printf '{"iat":%d,"exp":%d,"iss":"%s"}' $((now - 60)) $((now + 540)) "$app_id" | base64url)
	signature=$(printf '%s.%s' "$header" "$payload" | openssl dgst -sha256 -sign "$app_key" -binary | base64url)
	jwt="$header.$payload.$signature"
	installation_id=$(api GET "/repos/$public_repo/installation" "Bearer $jwt" | jq -er .id) ||
		die "the GitHub App is not installed on $public_repo"
	token=$(api POST "/app/installations/$installation_id/access_tokens" "Bearer $jwt" \
		-H "Content-Type: application/json" \
		-d "{\"repositories\":[\"${public_repo#*/}\"],\"permissions\":$permissions}" | jq -er .token) ||
		die "could not create an installation token"
	auth_header="AUTHORIZATION: basic $(printf 'x-access-token:%s' "$token" | openssl base64 -A)"
}

# 公開してはいけない語です。gitの名前とemail（global設定と、このcloneで有効な値）、GitHubの
# noreply emailの利用者番号、実行userの名前とhome directoryに、設定directoryのforbidden-words
# を加えます。公開用の名前・emailとrepository名に含まれる語と、4文字未満の語は使いません。
words_file=$work/forbidden-words
word_count=0
add_word() {
	local word=$1
	[[ ${#word} -ge 4 ]] || return 0
	[[ $identity == *"$word"* || $public_repo == *"$word"* ]] && return 0
	printf '%s\n' "$word" >>"$words_file"
	word_count=$((word_count + 1))
}
add_email() {
	local email=$1 local_part
	add_word "$email"
	local_part=${email%@*}
	add_word "$local_part"
	if [[ $email == *@users.noreply.github.com && $local_part == *+* ]]; then
		add_word "${local_part%%+*}"
		add_word "${local_part#*+}"
	fi
}
collect_words() {
	local word
	: >"$words_file"
	add_word "$(git config --global user.name 2>/dev/null || true)"
	add_email "$(git config --global user.email 2>/dev/null || true)"
	add_word "$(git config user.name 2>/dev/null || true)"
	add_email "$(git config user.email 2>/dev/null || true)"
	add_word "$(id -un 2>/dev/null || true)"
	add_word "$HOME"
	if [[ -r $config_dir/forbidden-words ]]; then
		while IFS= read -r word || [[ -n $word ]]; do
			word=${word%$'\r'}
			[[ -z $word || $word == \#* ]] || add_word "$word"
		done <"$config_dir/forbidden-words"
	fi
	[[ $word_count -gt 0 ]] || die "no forbidden words were found"
}

# matchesは、grepに一致すればtrueを返します。grepが失敗した場合も、検査できなかったものとして
# trueを返し、送りません。
matches() {
	local status=0
	grep -q "$@" || status=$?
	[[ $status -ne 1 ]]
}

# has_forbidden_wordは、標準入力に禁止語があればtrueを返します。
has_forbidden_word() { matches -i -F -f "$words_file"; }

check_failed=false
report() {
	echo "  $1: $2" >&2
	check_failed=true
}

# header_problemsは、commitを$work/objectへ書き出し、headerの問題を1行に1つ書きます。作者と
# committerは公開用の名前とemailで、日時はUTCでなければなりません。署名（gpgsig）やmergeで
# 取り込んだtag（mergetag）などの他のheaderは、名前や鍵を含むことがあるため認めません。
header_problems() {
	local commit=$1 role line
	git cat-file commit "$commit" >"$work/object"
	for role in author committer; do
		line=$(awk -v role="$role" '$0 == "" { exit } $1 == role { sub(/^[^ ]+ /, ""); print; exit }' "$work/object")
		[[ ${line% * *} == "$identity" ]] || echo "the $role is ${line% * *}"
		[[ ${line##* } == "+0000" ]] || echo "the $role date is not UTC (${line##* })"
	done
	awk '$0 == "" { exit } /^[^ ]/ && $1 != "tree" && $1 != "parent" && $1 != "author" && $1 != "committer" {
		print "it has a " $1 " header"
	}' "$work/object" | sort -u
}

# check_commitは、commitを送ってよいかを検査し、問題をreportで書きます。
check_commit() {
	local commit=$1 short problem status private_files
	short=$(git rev-parse --short "$commit")
	while IFS= read -r problem; do
		[[ -z $problem ]] || report "$short" "$problem"
	done <<<"$(header_problems "$commit")"

	sed '1,/^$/d' "$work/object" >"$work/message"
	! has_forbidden_word <"$work/message" || report "$short" "the message contains a forbidden word"
	! matches -i -E '^[[:alnum:]-]+-by:' "$work/message" ||
		report "$short" "the message has a trailer that names a person, such as Co-authored-by"

	git -c core.quotePath=false ls-tree -r --name-only "$commit" >"$work/paths"
	! has_forbidden_word <"$work/paths" || report "$short" "a file name contains a forbidden word"
	private_files=$(grep -E '(^|/)\.env($|\.)|(^|/)\.(aws|agents|codex|vscode|claude)/|\.pem$|(^|/)id_(rsa|ecdsa|ed25519)' \
		"$work/paths" | grep -vxF .env.example || true)
	[[ -z $private_files ]] || report "$short" "it contains private files: $(printf '%s\n' "$private_files" | tr '\n' ' ')"
	status=0
	git grep -a -i -F -l -f "$words_file" "$commit" -- . >"$work/matches" || status=$?
	case $status in
	0) report "$short" "files contain a forbidden word: $(sed "s/^$commit://" "$work/matches" | tr '\n' ' ')" ;;
	1) ;;
	*) report "$short" "could not search the files for forbidden words" ;;
	esac
}

# fetch_branchesは、GitHubのbranchをrefs/remotes/origin/へ取得し、remote_mainとlocal_mainを
# 決めます。
fetch_branches() {
	isolated_git fetch -q --prune --no-tags "$remote_url" '+refs/heads/*:refs/remotes/origin/*' ||
		die "cannot fetch from $remote_url"
	remote_main=$(git rev-parse -q --verify "refs/remotes/origin/main^{commit}") ||
		die "$public_repo has no main branch"
	local_main=$(git rev-parse -q --verify "refs/heads/main^{commit}") || die "this clone has no main branch"
}

# require_fast_forwardは、local mainがGitHubのmainを含むことを確かめます。
require_fast_forward() {
	git merge-base --is-ancestor "$remote_main" "$local_main" ||
		die "local main does not contain origin/main; rebase it (git rebase origin/main), run fix, then push"
}

# copy_mainは、local mainを空のbare repositoryへ取り込みます。GitHubへは、このrepositoryから
# 送ります。作業用のrepositoryの設定やhookを、送るときに使わないためです。
copy_main() {
	local repository=$1
	isolated_git init -q --bare "$repository"
	isolated_git -C "$repository" fetch -q --no-tags "$repo_root" "+refs/heads/main:refs/heads/main"
	[[ $(isolated_git -C "$repository" rev-parse refs/heads/main) == "$local_main" ]] ||
		die "main changed while checking; run the command again"
}

# verify_commitは、送ったcommitの作者とcommitterを、GitHubがAppのbotとして扱っているかを確かめます。
verify_commit() {
	local commit=$1 response author committer
	response=$(api GET "/repos/$public_repo/commits?sha=$commit&per_page=1" "token $token") ||
		die "pushed, but could not read $commit from GitHub; check the repository"
	author=$(jq -r --arg sha "$commit" '.[0] | select(.sha == $sha) | .author.login // empty' <<<"$response") || author=""
	committer=$(jq -r --arg sha "$commit" '.[0] | select(.sha == $sha) | .committer.login // empty' <<<"$response") ||
		committer=""
	[[ $author == "$bot_login" && $committer == "$bot_login" ]] ||
		die "pushed, but GitHub attributes $commit to ${author:-no account} and ${committer:-no account}; check the repository"
}

# verify_pusherは、repositoryのactivityで、pushした人がAppのbotとして記録されたかを確かめます。
# activityの反映は遅れることがあるため、数回読み直します。
verify_pusher() {
	local commit=$1 attempt actor=""
	for attempt in 1 2 3 4 5; do
		actor=$(api GET "/repos/$public_repo/activity?ref=refs/heads/main&per_page=20" "token $token" 2>/dev/null |
			jq -r --arg after "$commit" 'map(select(.after == $after)) | .[0].actor.login // empty' 2>/dev/null) ||
			actor=""
		[[ -z $actor && $attempt -lt 5 ]] || break
		sleep 2
	done
	if [[ -z $actor ]]; then
		warn "could not find the push of $commit in the repository activity; check who pushed it"
		return 0
	fi
	[[ $actor == "$bot_login" ]] || die "GitHub records the push of $commit as made by $actor; check the repository"
	echo "pushed by:  $actor"
}

cmd_setup() {
	local origin_url url repo hook
	origin_url=$(git config --get remote.origin.url) ||
		die "this clone has no origin; add it with: git remote add origin $remote_url"
	url=$(printf '%s' "$origin_url" | tr '[:upper:]' '[:lower:]')
	url=${url%/}
	url=${url%.git}
	repo=$(printf '%s' "$public_repo" | tr '[:upper:]' '[:lower:]')
	case $url in
	*github.com[:/]"$repo") ;;
	*) die "origin is $origin_url, not $remote_url" ;;
	esac
	[[ -z $(git config --get core.hooksPath || true) ]] ||
		die "core.hooksPath is set; this clone needs its own .git/hooks for the pre-push hook"
	hook=$(git rev-parse --git-path hooks/pre-push)
	if [[ -e $hook ]] && ! grep -q -F "$hook_marker" "$hook"; then
		die "$hook already exists; remove it or merge it with the hook that setup writes"
	fi

	git config user.name "$publish_name"
	git config user.email "$bot_email"
	git config commit.gpgSign false
	git config tag.gpgSign false
	git config remote.origin.pushurl "$no_push_url"
	mkdir -p "$(dirname "$hook")"
	cat >"$hook" <<HOOK
#!/bin/sh
# $hook_marker: tools/publish/publish.sh setupが作りました。
# $public_repo へは、tools/publish/publish.sh pushだけで送ります。git pushでは利用者の
# GitHubの認証情報が使われ、その利用者がpushしたと記録されるため、ここで止めます。
url=\$(printf '%s' "\$2" | tr '[:upper:]' '[:lower:]')
url=\${url%/}
case \${url%.git} in
*github.com[:/]$repo)
	echo "pre-push: push to $public_repo with tools/publish/publish.sh push" >&2
	exit 1
	;;
esac
exit 0
HOOK
	chmod +x "$hook"

	echo "identity:   $identity (commits and tags are not signed)"
	echo "origin:     $origin_url for fetch; git push origin is disabled"
	echo "pre-push:   $hook refuses git push to $public_repo"
	echo "Commit with TZ=UTC git commit, or run tools/publish/publish.sh fix before push."
}

cmd_fetch() {
	get_token '{"contents":"read"}'
	isolated_git fetch --prune --no-tags "$remote_url" '+refs/heads/*:refs/remotes/origin/*' ||
		die "cannot fetch the branches from $remote_url"
	isolated_git fetch --no-tags "$remote_url" 'refs/tags/*:refs/tags/*' || die "cannot fetch the tags from $remote_url"
	git log -1 --format='origin/main: %h %s' refs/remotes/origin/main
}

cmd_push() {
	local commit commits count
	get_token '{"contents":"write","workflows":"write"}'
	collect_words
	fetch_branches
	if [[ $local_main == "$remote_main" ]]; then
		echo "main is up to date with $public_repo"
		return 0
	fi
	require_fast_forward
	commits=$(git rev-list --reverse "$remote_main..$local_main")
	for commit in $commits; do
		check_commit "$commit"
	done
	! $check_failed ||
		die "nothing was pushed; run tools/publish/publish.sh fix for identities, dates and headers, and amend or rebase for the rest"
	count=$(git rev-list --count "$remote_main..$local_main")
	echo "repository: $public_repo (main $(git rev-parse --short "$remote_main"))"
	echo "commits:    $count, made by $identity"
	git log --reverse --format='  %h %s' "$remote_main..$local_main"
	echo "checks:     identity, UTC dates, headers, trailers, $word_count forbidden words, private files: passed"
	if $dry_run; then
		echo "dry run: rerun without --dry-run to push"
		return 0
	fi
	copy_main "$work/push.git"
	isolated_git -C "$work/push.git" push "$remote_url" "$local_main:refs/heads/main" ||
		die "the push was refused; run tools/publish/publish.sh fetch and check origin/main"
	git update-ref refs/remotes/origin/main "$local_main"
	verify_commit "$local_main"
	verify_pusher "$local_main"
	echo "pushed:     https://github.com/$public_repo/commit/$local_main"
}

cmd_fix() {
	local commit parent problems tree author_time committer_time rewritten=false state
	for state in rebase-merge rebase-apply MERGE_HEAD CHERRY_PICK_HEAD REVERT_HEAD; do
		[[ ! -e $(git rev-parse --git-path "$state") ]] || die "finish or abort the rebase, merge, cherry-pick or revert first"
	done
	get_token '{"contents":"read"}'
	fetch_branches
	if [[ $local_main == "$remote_main" ]]; then
		echo "main has no commits that $public_repo does not have"
		return 0
	fi
	require_fast_forward
	[[ -z $(git rev-list --min-parents=2 "$remote_main..$local_main") ]] ||
		die "fix does not rewrite merge commits; rebase main onto origin/main first"
	# 直す必要のないcommitはそのまま使い、最初に直したcommitより後は、親が変わるためすべて作り直します。
	parent=$remote_main
	for commit in $(git rev-list --reverse "$remote_main..$local_main"); do
		problems=$(header_problems "$commit")
		if ! $rewritten && [[ -z $problems ]]; then
			parent=$commit
			continue
		fi
		rewritten=true
		if $dry_run; then
			echo "  would rewrite $(git log -1 --format='%h %s' "$commit")"
			continue
		fi
		tree=$(git rev-parse "$commit^{tree}")
		author_time=$(git log -1 --format=%at "$commit")
		committer_time=$(git log -1 --format=%ct "$commit")
		parent=$(sed '1,/^$/d' "$work/object" |
			GIT_AUTHOR_NAME=$publish_name GIT_AUTHOR_EMAIL=$bot_email GIT_AUTHOR_DATE="@$author_time +0000" \
				GIT_COMMITTER_NAME=$publish_name GIT_COMMITTER_EMAIL=$bot_email \
				GIT_COMMITTER_DATE="@$committer_time +0000" \
				git commit-tree --no-gpg-sign "$tree" -p "$parent" -F -)
		echo "  $(git rev-parse --short "$commit") -> $(git log -1 --format='%h %s' "$parent")"
	done
	if ! $rewritten; then
		echo "every commit already has the publisher identity, UTC dates and no extra headers"
		return 0
	fi
	if $dry_run; then
		echo "dry run: rerun without --dry-run to rewrite"
		return 0
	fi
	git update-ref -m "publish.sh fix" refs/heads/main "$parent" "$local_main"
	echo "main:       $(git rev-parse --short "$local_main") -> $(git rev-parse --short "$parent")"
}

cmd_release() {
	local tag_object line extra
	get_token '{"contents":"write","workflows":"write"}'
	collect_words
	fetch_branches
	[[ -z $(isolated_git ls-remote --tags "$remote_url" "refs/tags/$version") ]] ||
		die "$version already exists in $public_repo"
	! git rev-parse -q --verify "refs/tags/$version" >/dev/null ||
		die "this clone already has a tag $version; delete it (git tag -d $version) if it was not published"
	[[ $local_main == "$remote_main" ]] ||
		die "local main and origin/main differ; push or rebase first so that the tag is on the pushed main"
	copy_main "$work/tag.git"
	# tagの作者（tagger）は、gitがcommitterの環境変数から決めます。
	export GIT_COMMITTER_NAME=$publish_name GIT_COMMITTER_EMAIL=$bot_email GIT_COMMITTER_DATE="@$(date +%s) +0000"
	isolated_git -C "$work/tag.git" -c tag.gpgSign=false tag -a "$version" -m "heterolog $version" "$local_main"
	unset GIT_COMMITTER_NAME GIT_COMMITTER_EMAIL GIT_COMMITTER_DATE
	isolated_git -C "$work/tag.git" cat-file tag "$version" >"$work/object"
	line=$(awk '$0 == "" { exit } $1 == "tagger" { sub(/^[^ ]+ /, ""); print; exit }' "$work/object")
	[[ ${line% * *} == "$identity" && ${line##* } == "+0000" ]] || die "the tag was not made by $identity in UTC"
	extra=$(awk '$0 == "" { exit } $1 != "object" && $1 != "type" && $1 != "tag" && $1 != "tagger" { print $1 }' "$work/object")
	[[ -z $extra ]] || die "the tag has extra headers: $extra"
	sed '1,/^$/d' "$work/object" >"$work/message"
	! has_forbidden_word <"$work/message" || die "the tag message contains a forbidden word"
	echo "repository: $public_repo"
	echo "tag:        $version on $(git log -1 --format='%h %s' "$local_main")"
	echo "tagger:     $identity"
	echo "checks:     tagger, UTC date, headers, $word_count forbidden words: passed"
	if $dry_run; then
		echo "dry run: rerun without --dry-run to push the tag"
		return 0
	fi
	isolated_git -C "$work/tag.git" push "$remote_url" "refs/tags/$version:refs/tags/$version" ||
		die "the tag push was refused"
	tag_object=$(isolated_git -C "$work/tag.git" rev-parse "refs/tags/$version")
	[[ $(api GET "/repos/$public_repo/git/ref/tags/$version" "token $token" | jq -r .object.sha) == "$tag_object" ]] ||
		die "pushed, but GitHub does not show the tag object $tag_object for $version; check the repository"
	isolated_git fetch -q --no-tags "$work/tag.git" "refs/tags/$version:refs/tags/$version"
	echo "released:   https://github.com/$public_repo/releases/tag/$version"
}

case $command in
setup) cmd_setup ;;
fetch) cmd_fetch ;;
push) cmd_push ;;
fix) cmd_fix ;;
release) cmd_release ;;
esac
