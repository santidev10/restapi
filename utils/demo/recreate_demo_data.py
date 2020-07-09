from aw_reporting.demo.recreate_demo_data import recreate_demo_data
from utils.demo.recreate_demo_source_data import recreate_demo_source_data


def recreate_test_demo_data():
    recreate_demo_source_data()
    recreate_demo_data()
