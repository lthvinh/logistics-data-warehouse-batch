from pathlib import Path
import sys

sys.path.append(str(Path(__file__).parent.parent.parent / 'transform'))

from curated_data_transformer import DimDate

def main():
    dim_date = DimDate()
    dim_date.process_dim_date()
    
if __name__ == '__main__':
    main()