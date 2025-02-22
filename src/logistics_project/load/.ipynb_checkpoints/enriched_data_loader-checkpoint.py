from pathlib import Path
import sys

sys.path.append(Path.cwd() / 'decorator')

from decorator_factory import DecoratorFactory

class EnrichedDataLoader:

    @staticmethod
    def load_data(self, df, df_name, output_path, mode, format, partition_columns, compression, extra_configs):
        
        writer = df.write.mode(mode)

        if compression:
            writer = writer.option("compression", compression)
        if partition_columns:
            writer = writer.partitionBy(partition_columns)
        if extra_configs:
            writer = writer.options(**extra_configs)
        
        writer.format(format).save(output_path)
        self.logger.info(f"Path: {output_path}")

    
            