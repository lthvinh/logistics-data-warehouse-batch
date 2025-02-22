from pathlib import Path
import sys

sys.path.append(str(Path(__file__).parent.parent.parent / 'transform'))

from curated_data_transformer import FactShipments

def main():
    fact_orders = FactShipments()
    fact_orders.process_fact_shipments()

if __name__ == '__main__':
    main()