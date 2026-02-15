import sys
from client import run_data_collection
from normalizer import run_field_normalization
from analyzer import run_data_analysis
from classifier import run_classification
from classification_visualiser import run_visualization

        

if __name__ == "__main__":
    run_data_collection()
    
    run_field_normalization()
    
    run_data_analysis()
    
    run_classification()
    
    run_visualization()
    
    