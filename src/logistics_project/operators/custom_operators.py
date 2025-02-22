from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from pathlib import Path
import yaml

class CustomSparkSubmitOperator(SparkSubmitOperator):
    def execute(self, context):
        
        base_path = Path(__file__).parent.parent 
        config_path = base_path / 'config/config.yaml'
        workflows_path = base_path / 'workflows'
        
        with open(config_path) as file:
            config = yaml.safe_load(file)
            packages = config['spark']['packages']
            
        self.packages = packages
        self.application = str(workflows_path / f'{self.application}.py')
        
        return super().execute(context)