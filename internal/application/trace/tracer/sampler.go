package tracer

import (
	"math/rand"

	"github.com/go-jappe/heterolog/internal/application/trace/span"
)

// Sampler はサンプリング判定を行うインターフェース
type Sampler interface {
	// ShouldSample はトレースをサンプリングするかどうかを判定する
	ShouldSample(parameters SamplingParameters) bool
}

// SamplingParameters はサンプリング判定に使用するパラメータ
type SamplingParameters struct {
	ParentContext *SpanContext
	Name          string
	Kind          span.SpanKind
	Attributes    []span.Attribute
}

// alwaysSampler は常にサンプリングするSampler
type alwaysSampler struct{}

// ShouldSample は常にtrueを返す
func (s alwaysSampler) ShouldSample(parameters SamplingParameters) bool {
	return true
}

// AlwaysSample は常にサンプリングするSamplerを返す
func AlwaysSample() Sampler {
	return alwaysSampler{}
}

// neverSampler は常にサンプリングしないSampler
type neverSampler struct{}

// ShouldSample は常にfalseを返す
func (s neverSampler) ShouldSample(parameters SamplingParameters) bool {
	return false
}

// NeverSample は常にサンプリングしないSamplerを返す
func NeverSample() Sampler {
	return neverSampler{}
}

// probabilitySampler は確率的にサンプリングするSampler
type probabilitySampler struct {
	probability float64
}

// ShouldSample は確率的にサンプリングするかどうかを判定する
func (s probabilitySampler) ShouldSample(parameters SamplingParameters) bool {
	// 親Spanがサンプリングされている場合は、子Spanもサンプリングする
	if parameters.ParentContext != nil && parameters.ParentContext.TraceFlags&1 == 1 {
		return true
	}

	// 確率的にサンプリング
	return rand.Float64() < s.probability
}

// ProbabilitySample は確率的にサンプリングするSamplerを返す
func ProbabilitySample(probability float64) Sampler {
	if probability <= 0 {
		return NeverSample()
	}
	if probability >= 1 {
		return AlwaysSample()
	}
	return probabilitySampler{probability: probability}
}

// parentBasedSampler は親Spanのサンプリング状態に基づいてサンプリングするSampler
type parentBasedSampler struct {
	root Sampler
}

// ShouldSample は親Spanのサンプリング状態に基づいてサンプリングするかどうかを判定する
func (s parentBasedSampler) ShouldSample(parameters SamplingParameters) bool {
	// 親Spanがない場合は、rootサンプラーで判定
	if parameters.ParentContext == nil {
		return s.root.ShouldSample(parameters)
	}

	// 親Spanがサンプリングされている場合は、子Spanもサンプリングする
	return parameters.ParentContext.TraceFlags&1 == 1
}

// ParentBasedSample は親Spanのサンプリング状態に基づいてサンプリングするSamplerを返す
func ParentBasedSample(root Sampler) Sampler {
	if root == nil {
		root = AlwaysSample()
	}
	return parentBasedSampler{root: root}
}
