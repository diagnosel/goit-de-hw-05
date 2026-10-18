"""Check strict limits without starting Kafka."""
import unittest
from config import TEMPERATURE, HUMIDITY
from process_sensors import build_alerts


class ThresholdTests(unittest.TestCase):
    def test_boundaries_and_both_alerts(self):
        cases = [(40, 20, []), (40, 80, []), (39.99, 50, []),
                 (40.01, 50, [TEMPERATURE]), (30, 19.99, [HUMIDITY]),
                 (30, 80.01, [HUMIDITY]), (45, 15, [TEMPERATURE, HUMIDITY]),
                 (25, 85, [HUMIDITY])]
        for temperature, humidity, expected in cases:
            with self.subTest(temperature=temperature, humidity=humidity):
                reading = {"sensor_id": "boundary-test", "timestamp": "test-only",
                           "event_id": "test-only", "temperature": temperature,
                           "humidity": humidity}
                alerts = build_alerts(reading, "test-topic:0:7")
                self.assertEqual([topic for topic, _ in alerts], expected)
                for _, alert in alerts:
                    self.assertEqual(alert["sensor_id"], reading["sensor_id"])
                    self.assertEqual(alert["timestamp"], reading["timestamp"])
                    self.assertEqual(alert["temperature"], temperature)
                    self.assertEqual(alert["humidity"], humidity)
                    self.assertEqual(alert["source"], "test-topic:0:7")
                    self.assertIn(alert["alert_type"], alert["alert_id"])
                    self.assertTrue(alert["message"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
