import json
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

def plot_decision_boundary(json_path):
    # Load Data
    with open(json_path, 'r') as f:
        data = json.load(f)
    
    df = pd.DataFrame(data)
    df = df.sort_values(by="score").reset_index(drop=True)
    plt.figure(figsize=(18, 10)) 
    sns.set_style("whitegrid")

    colors = {"SQL": "#19C719", "MONGO": "#951204", "BOTH": "#2340a1"}
    
    sns.scatterplot(
        data=df, 
        x=df.index, 
        y="score", 
        hue="decision", 
        palette=colors, 
        s=100, 
        edgecolor="black",
        alpha=0.8
    )

    threshold = 0.3
    plt.axhline(y=threshold, color='r', linestyle='--', linewidth=2, label=f'Threshold ({threshold})')

    plt.title("Field Distribution: SQL vs MongoDB Decision Score", fontsize=16)
    plt.ylabel("Mongo Score (Higher = Better for Mongo)", fontsize=12)
    plt.xlabel("Fields (Sorted by Score)", fontsize=12)
    

    for i, row in df.iterrows():
        plt.text(
            x=i, 
            y=row['score'] + 0.05,
            s=row['fieldName'], 
            fontsize=9, 
            rotation=90,      
            ha='center',     
            va='bottom',      
            color='#333333'   
        )

    plt.legend(title="Decision", loc='upper left')
    plt.tight_layout()
    plt.savefig('data/decision_graph.png')

def run_visualization():
    plot_decision_boundary('data/field_metadata.json')