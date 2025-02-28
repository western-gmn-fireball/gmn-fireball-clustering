import unittest
from fireball_clustering.data_processing.clustering import filterFireballsWithFR
import datetime

class TestFilterFireballs(unittest.TestCase):
    def setUp(self) -> None:

        self.fireballs = [
            (datetime.datetime.now(), datetime.datetime.now() + datetime.timedelta(hours=1), 'Fireball1'),
            (datetime.datetime.now() + datetime.timedelta(seconds=9), datetime.datetime.now() + datetime.timedelta(hours=2), 'Fireball2'),
            (datetime.datetime.now() + datetime.timedelta(hours=10), datetime.datetime.now() + datetime.timedelta(hours=3), 'Fireball3'),
            (datetime.datetime.now() + datetime.timedelta(seconds=11), datetime.datetime.now() + datetime.timedelta(hours=3), 'Fireball4'),
            (datetime.datetime.now() + datetime.timedelta(seconds=11), datetime.datetime.now() + datetime.timedelta(hours=3), 'Fireball5'),
            (datetime.datetime.now() - datetime.timedelta(hours=2), datetime.datetime.now() + datetime.timedelta(hours=3), 'Fireball6')
        ]
        self.fr_timestamps = [
            datetime.datetime.now() - datetime.timedelta(hours=2),
            datetime.datetime.now()
        ]

    def testFilterFireballs(self):
        results = filterFireballsWithFR(self.fireballs, self.fr_timestamps)
        print(results)
        candidates = [fireball for _, _, fireball in results] 
        expected_result = ['Fireball1', 'Fireball2', 'Fireball6']
        self.assertEqual(expected_result, candidates)

if __name__=='__main__':
    unittest.main()
