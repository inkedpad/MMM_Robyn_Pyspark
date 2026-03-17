# MMM_Robyn — Marketing Mix Modelling Pipeline

Python-based MMM pipeline using the [Robyn](https://github.com/facebookexperimental/Robyn) package. Runs multi-segment model training, Pareto optimization, clustering, model selection scoring, and budget allocation.

---

## Prerequisites

### 1. Python
Python 3.11 (64-bit) — [download here](https://www.python.org/downloads/)

### 2. R (required for glmnet dependency)
Install R ≥ 4.0 — [download here](https://cran.r-project.org/)

Then open the R console and run:
```r
install.packages("glmnet")
```

### 3. Virtual environment + dependencies
From the `Codes/` directory:
```bash
python -m venv .venv
.venv\Scripts\activate        # Windows
pip install -r Requirements.txt
```

---

## ⚠️ Required Manual Patch (Robyn Bug Fix)

Before running, you must patch one file in the Robyn package. This fixes a known bug in Pareto plot generation.

**Open this file:**
```
.venv\Lib\site-packages\robyn\modeling\pareto\plot_data_generator.py
```

**Find this block:**
```python
        # Check if 'revenue' exists in the columns
        if "revenue" in dt_saturated_dfs.dt_modSaturated.columns:
            # Rename 'revenue' to 'dep_var'
            dt_saturated_dfs.dt_modSaturated = dt_saturated_dfs.dt_modSaturated.rename(
                columns={"revenue": "dep_var"}
            )
            # print("Column 'revenue' renamed to 'dep_var'.")
        else:
            # print("Column 'revenue' does not exist.")
            pass
```

**Replace with:**
```python
        # Check if dep_var or 'revenue' exists in the columns and rename to 'dep_var'
        dep_var_name = self.mmm_data.mmmdata_spec.dep_var
        if dep_var_name in dt_saturated_dfs.dt_modSaturated.columns:
            dt_saturated_dfs.dt_modSaturated = dt_saturated_dfs.dt_modSaturated.rename(
                columns={dep_var_name: "dep_var"}
            )
        elif "revenue" in dt_saturated_dfs.dt_modSaturated.columns:
            dt_saturated_dfs.dt_modSaturated = dt_saturated_dfs.dt_modSaturated.rename(
                columns={"revenue": "dep_var"}
            )
```

This patch must be re-applied any time the Robyn package is reinstalled or updated.

---

## Configuration

All parameters are set in `Codes/config.py`. Edit this file before running:

- Input data path
- Segment columns (region, brand, category)
- Paid media channels
- Date range
- Model hyperparameters (trials, iterations, cores)
- Output paths

---

## Running the Pipeline

Run scripts in this order from the `Codes/` directory with the venv activated:

**Step 1 — Preprocessing:**
```bash
python Robyn_MMM_Premodelling.py
```

**Step 2 — Modelling (training + scoring + budget allocation):**
```bash
python Robyn_MMM_Modelling.py
```

---

## Output Structure

```
Codes/
├── Modelling_Output/
│   ├── {segment}/              # Per-segment Pareto plots and one-pagers
│   ├── best_model_selections.json
│   ├── hill_params_all_segments.json
│   └── model_selection_scores.csv
├── Preprocessing_Output/
│   └── Final_Processed_Data.csv
└── premodelling_output/
    └── modelling_data.csv
```

---

## Pipeline Steps (what the code does)

1. **Data prep** — loads raw data, filters to relevant columns, splits by segment
2. **Training** — runs Robyn MMM for each segment (geometric adstock, Hill saturation, Prophet decomposition)
3. **Pareto optimization** — finds non-dominated solutions on NRMSE vs DECOMP.RSSD frontier
4. **Clustering** — clusters Pareto solutions by hyperparameters
5. **One-pager generation** — saves diagnostic plots per segment
6. **Model selection** — business-first scorer selects best solution per segment
7. **Budget allocation** — scipy optimizer finds optimal channel spend using Hill response curves
   - Scenario 1: Maximum response at current budget
   - Scenario 2: Minimum spend to maintain current response

---

## Notes

- R and glmnet are required only for model training (used internally by Robyn via rpy2)
- Budget allocation is implemented via a custom scipy optimizer (Robyn's built-in optimizer is not yet implemented in the Python version)
- The pipeline is designed to scale to all 36 segments (4 regions × 3 categories × 3 brands)