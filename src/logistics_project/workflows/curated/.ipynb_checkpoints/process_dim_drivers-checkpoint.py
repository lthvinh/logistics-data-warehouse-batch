from pathlib import Path
import sys

sys.path.append(str(Path(__file__).parent.parent.parent / 'transform'))

from curated_data_transformer import DimDrivers

def main():
    dim_drivers = DimDrivers()
    dim_drivers.process_dim_drivers()

if __name__ == '__main__':
    main()