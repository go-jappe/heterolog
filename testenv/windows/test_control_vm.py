"""VMを起動せず、認証情報も使わずにキーボード準備を検証する。"""

import importlib.util
from pathlib import Path
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("control_vm", Path(__file__).with_name("control-vm.py"))
control_vm = importlib.util.module_from_spec(spec)
spec.loader.exec_module(control_vm)


class KeyboardTest(unittest.TestCase):
    def setUp(self):
        self.control = control_vm.VMControl.__new__(control_vm.VMControl)
        self.control.state_directory = Path("unused-test-directory")
        self.actions = []
        self.control.key = lambda keys: self.actions.append(("key", keys))
        self.control.key_event = lambda key, down: self.actions.append(("event", key, down))

    def test_numlock_is_enabled_only_when_off(self):
        with patch.object(control_vm, "keyboard_leds", side_effect=[0, 2]):
            self.control.prepare_keyboard()
        self.assertEqual(self.actions, [("key", ["num_lock"])])

    def test_already_correct_state_is_not_toggled(self):
        with patch.object(control_vm, "keyboard_leds", return_value=2):
            self.control.prepare_keyboard()
        self.assertEqual(self.actions, [])

    def test_capslock_is_disabled_without_changing_numlock(self):
        with patch.object(control_vm, "keyboard_leds", side_effect=[6, 2]):
            self.control.prepare_keyboard()
        self.assertEqual(self.actions, [("key", ["caps_lock"])])

    def test_both_locks_are_corrected(self):
        with patch.object(control_vm, "keyboard_leds", side_effect=[4, 2]):
            self.control.prepare_keyboard()
        self.assertEqual(self.actions, [("key", ["caps_lock"]), ("key", ["num_lock"])])

    def test_each_input_rechecks_state(self):
        with patch.object(control_vm, "keyboard_leds", side_effect=[2, 2, 0, 2]):
            self.control.type_text("a")
            self.control.type_text("b")
        self.assertEqual(self.actions, [("key", ["a"]), ("key", ["num_lock"]), ("key", ["b"])])

    def test_symbol_input_follows_numlock_confirmation(self):
        with patch.object(control_vm, "keyboard_leds", side_effect=[0, 2]):
            self.control.type_text("!")
        self.assertEqual(self.actions, [
            ("key", ["num_lock"]), ("event", "alt", True),
            ("key", ["kp_0"]), ("key", ["kp_0"]),
            ("key", ["kp_3"]), ("key", ["kp_3"]), ("event", "alt", False),
        ])

    def test_unconfirmed_state_aborts_before_value_input(self):
        with patch.object(control_vm, "keyboard_leds", return_value=0):
            with self.assertRaisesRegex(RuntimeError, "input aborted"):
                self.control.type_text("not-a-real-password")
        self.assertEqual(self.actions, [("key", ["num_lock"])])

    def test_query_error_does_not_type(self):
        with patch.object(control_vm, "keyboard_leds", side_effect=TimeoutError):
            with self.assertRaises(TimeoutError):
                self.control.type_text("not-a-real-password")
        self.assertEqual(self.actions, [])

    def test_click_coordinates_and_release(self):
        self.control.call = lambda command, args: self.actions.append(args["events"])
        with patch.object(control_vm.time, "sleep"):
            self.control.click(799, 599, 800, 600)
        self.assertEqual(self.actions, [
            [{"type": "abs", "data": {"axis": "x", "value": 32767}},
             {"type": "abs", "data": {"axis": "y", "value": 32767}}],
            [{"type": "btn", "data": {"button": "left", "down": True}}],
            [{"type": "btn", "data": {"button": "left", "down": False}}],
        ])

    def test_click_rejects_outside_coordinates(self):
        self.control.call = lambda *args: self.fail("Invalid click must not send input")
        with self.assertRaises(ValueError):
            self.control.click(800, 599, 800, 600)


if __name__ == "__main__":
    unittest.main()
