from pathlib import Path
import sys

sys.path.append(str(Path(__file__).parent.parent.parent / 'transform'))

from curated_data_transformer import DimUsers

def main():
    dim_users = DimUsers()
    dim_users.process_dim_users()

if __name__ == '__main__':
    main()