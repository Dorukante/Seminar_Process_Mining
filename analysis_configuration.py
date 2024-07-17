import yaml
from pathlib import Path


class AnalysisConfiguration:
    def __init__(self, path=Path("config_analysis.yaml")):
        config = yaml.safe_load(open(path))

        self.dataset_name = config["dataset_name"]
        self.intermediate_output_directory = config["intermediate_output_directory"]
        self.final_output_directory = config["final_output_directory"]

        self.case_edges = config['case_edges']
        self.edge_min_freq = config['edge_min_freq']