from pathlib import Path
import sys

sys.path.append(str(Path(__file__).parent.parent.parent / 'transform'))

from curated_data_transformer import FactOrders

def main():
    fact_orders = FactOrders()
    fact_orders.process_fact_orders()

if __name__ == '__main__':
    main()