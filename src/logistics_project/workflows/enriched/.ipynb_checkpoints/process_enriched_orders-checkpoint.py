from pathlib import Path
import sys

sys.path.append(str(Path(__file__).parent.parent.parent / 'transform'))

from enriched_data_transformer import EnrichedDataTransformer

def main():
    enriched_data_transformer = EnrichedDataTransformer()
    enriched_data_transformer.process_enriched_orders()
    
if __name__ == '__main__':
    main()