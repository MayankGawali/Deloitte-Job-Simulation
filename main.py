import json
import unittest
import datetime

METRIC_TEMP = 'motor_temperature_c'
ISO_FORMAT = '%Y-%m-%dT%H:%M:%S.%fZ'

with open("./data-1.json", "r", encoding='utf-8') as f:
    jsonData1 = json.load(f)
with open("./data-2.json", "r",encoding='utf-8') as f:
    jsonData2 = json.load(f)
with open("./data-result.json", "r",encoding='utf-8') as f:
    jsonExpectedResult = json.load(f)

def convertFromFormat1(jsonObject):
    unified_records = []

    for record in jsonObject:
        if not isinstance(record, dict):
            continue

        timestamp_ms = record.get('timestamp')
        device_id = record.get('deviceID')

        location_path = record.get('location', '')
        try:
            plant_id = location_path.split('/')[3]
        except IndexError:
            plant_id = "UNKNOWN"

        if 'temp' in record and timestamp_ms and device_id:
            unified_records.append({
                'timestamp': timestamp_ms,
                'device_id': device_id,
                'metric_name': METRIC_TEMP,
                'metric_value': float(record['temp']),
                'plant_id': plant_id
            })
    return unified_records

def convertFromFormat2(jsonObject):
    unified_records = []
    for record in jsonObject:
        if not isinstance(record, dict):
            continue
        timestamp_str = record.get('timestamp')
        timestamp_ms = None
        if not isinstance(timestamp_str, str):
            continue
        try:
            dt_obj = datetime.datetime.strptime(timestamp_str, ISO_FORMAT)
            timestamp_ms = int(dt_obj.timestamp() * 1000)
        except (ValueError, KeyError, TypeError):
            continue
        device_id = record.get('device', {}).get('id')
        plant_id = record.get('factory')

        temp_data = record.get('data', {})

        if temp_data.get('temperature') is not None and device_id and plant_id:
            unified_records.append({
                'timestamp':
                timestamp_ms,
                'device_id':
                device_id,
                'metric_name':
                METRIC_TEMP,
                'metric_value':
                float(temp_data['temperature']),
                'plant_id':
                plant_id
            })
    return unified_records

def main(json_data_source):
    all_data = []
    all_data.extend(convertFromFormat1(jsonData1))
    all_data.extend(convertFromFormat2(jsonData2))

    def sort_key(item):
        return (item['timestamp'], item['device_id'], item['metric_name'])
    return sorted(all_data, key=sort_key)

class TestSolution(unittest.TestCase):
    def setUp(self):
        def sort_key(item):
            return (item['timestamp'], item['device_id'], item['metric_name'])
        if isinstance(jsonExpectedResult, list):
            self.expected_unified_result_sorted = sorted(jsonExpectedResult,
                                                         key=sort_key)
        else:
            self.expected_unified_result_sorted = []
    def run_conversion_test(self, json_data, failure_message):
        result = main(None)

        def sort_key(item):
            return (item['timestamp'], item['device_id'], item['metric_name'])

        result_sorted = sorted(result, key=sort_key)

        self.assertEqual(result_sorted, self.expected_unified_result_sorted,
                         failure_message)
    def test_dataType1(self):
        self.run_conversion_test(jsonData1, 'Converting from Type 1 failed')

    def test_dataType2(self):
        self.run_conversion_test(jsonData2, 'Converting from Type 2 failed')

if __name__ == '__main__':
    unittest.main(argv=['first-arg-is-ignored'], exit=False)
