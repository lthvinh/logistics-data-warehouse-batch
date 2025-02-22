from pathlib import Path
import sys

sys.path.append(str(Path(__file__).parent.parent.parent / 'transform'))

from curated_data_transformer import DimLocations

def main():
    dim_locations = DimLocations()
    dim_locations.process_dim_locations()

if __name__ == '__main__':
    main()