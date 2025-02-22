from pathlib import Path
import sys

sys.path.append(str(Path(__file__).parent.parent.parent / 'transform'))

from curated_data_transformer import FactPayments

def main():
    fact_orders = FactPayments()
    fact_orders.process_fact_payments()

if __name__ == '__main__':
    main()