import logging
import os
import json
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# Настройка логгера
logger = logging.getLogger(__name__)

def save_submission(ids, preds, output_dir, timestamp, id_col_name, target_col_name):
    sub = pd.DataFrame({id_col_name: ids, target_col_name: np.asarray(preds, dtype=int)})
    submission_path = f"sample_submission_{timestamp}.csv"
    sub.to_csv(os.path.join(output_dir, submission_path), index=False)
    logger.info(f"Saved submission to {submission_path}")

def save_feature_importances(top5, output_dir, timestamp):
    fimp_path = f"feature_importances_top5_{timestamp}.json"
    with open(os.path.join(output_dir, fimp_path), "w", encoding="utf-8") as f:
        json.dump(top5, f, ensure_ascii=False, indent=2)
    logger.info(f"Saved feature importances to {fimp_path}")

def save_density_plot(probs, output_dir, timestamp):
    density_path = f"score_density_{timestamp}.png"
    plt.figure()
    plt.hist(probs, bins=50, density=True)
    plt.xlabel("Predicted fraud probability")
    plt.ylabel("Density")
    plt.title("Score density")
    plt.savefig(os.path.join(output_dir, density_path), bbox_inches='tight')
    plt.close()
    logger.info(f"Saved density plots to {density_path}")
