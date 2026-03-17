import os
import json
import multiprocessing
import pandas as pd
import numpy as np
import traceback

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, BooleanType
)

from robyn.robyn import Robyn
from robyn.data.entities.mmmdata import MMMData
from robyn.data.entities.hyperparameters import Hyperparameters, ChannelHyperparameters
from robyn.data.entities.holidays_data import HolidaysData
from robyn.data.entities.enums import ProphetVariableType, ProphetSigns
from robyn.modeling.entities.modelrun_trials_config import TrialsConfig
from robyn.modeling.pareto.pareto_optimizer import ParetoOptimizer
from robyn.modeling.pareto.pareto_utils import ParetoUtils
from robyn.modeling.clustering.cluster_builder import ClusterBuilder, ClusteringConfig, ClusterBy, DependentVarType
import robyn as robyn_pkg


# ════════════════════════════════════════════════════════════════════════════
# SPARK SESSION
# On Databricks this already exists — builder call is a no-op
# ════════════════════════════════════════════════════════════════════════════

def get_spark():
    """Get or create SparkSession. No-op on Databricks where spark already exists."""
    return SparkSession.builder \
        .appName("Robyn_Modelling_Functions") \
        .getOrCreate()


# ════════════════════════════════════════════════════════════════════════════
# HOLIDAYS
# Unchanged — reads a small bundled CSV, no benefit to Spark here
# ════════════════════════════════════════════════════════════════════════════

def load_holidays():
    """Load bundled Robyn holidays CSV."""
    path = os.path.join(
        os.path.dirname(robyn_pkg.__file__),
        'tutorials', 'resources', 'dt_prophet_holidays.csv'
    )
    return pd.read_csv(path)


# ════════════════════════════════════════════════════════════════════════════
# REGISTRY PARSING
# Unchanged — pure Python dict operations, no data involved
# ════════════════════════════════════════════════════════════════════════════

def parse_registry(modelling_registry):
    """
    Extracts all modelling config from the modelling registry.

    Returns:
        dep_var            : str
        paid_media_spends  : list of spend column names
        paid_media_vars    : list of exposure column names (best already selected)
        context_vars       : list of control column names
    """
    dep_var = modelling_registry.get('global_response', [])[0]

    paid_media_spends, paid_media_vars = [], []
    for ch_name, ch_data in modelling_registry.get('channels', {}).items():
        spends    = ch_data.get('spend', [])
        exposures = ch_data.get('exposure', [])
        if not spends:
            print(f"[REGISTRY] WARNING — '{ch_name}' has no spend. Skipping.")
            continue
        if not exposures:
            print(f"[REGISTRY] WARNING — '{ch_name}' has no exposure. Skipping.")
            continue
        paid_media_spends.append(spends[0])
        paid_media_vars.append(exposures[0])

    context_vars = modelling_registry.get('global_controls', [])



    return dep_var, paid_media_spends, paid_media_vars, context_vars


# ════════════════════════════════════════════════════════════════════════════
# TRAIN
# Unchanged — Robyn is R-backed, must run on driver in pandas
# Cannot be distributed to Spark workers without R on each worker
# ════════════════════════════════════════════════════════════════════════════

def train_segment(
    df_seg, dep_var, paid_media_spends, paid_media_vars,
    context_vars, context_signs, output_dir, dt_holidays,
    prophet_country, iterations, trials, cores, adstock,
):
    window_start = str(df_seg['date'].min().date())
    window_end   = str(df_seg['date'].max().date())

    mmm_data_spec = MMMData.MMMDataSpec(
        dep_var=dep_var,
        dep_var_type='revenue',
        date_var='date',
        paid_media_spends=paid_media_spends,
        paid_media_vars=paid_media_vars,
        context_vars=context_vars,
        context_signs=context_signs,
        organic_vars=[],
        window_start=window_start,
        window_end=window_end,
    )
    mmm_data = MMMData(data=df_seg, mmmdata_spec=mmm_data_spec)

    hyper_params = {
        ch: ChannelHyperparameters(thetas=[0.0, 0.8], alphas=[0.5, 3.0], gammas=[0.3, 1.0])
        for ch in paid_media_spends
    }
    hyperparameters = Hyperparameters(adstock=adstock, hyperparameters=hyper_params)

    holidays_data = HolidaysData(
        dt_holidays=dt_holidays,
        prophet_vars=[ProphetVariableType.TREND, ProphetVariableType.SEASON, ProphetVariableType.HOLIDAY],
        prophet_country=prophet_country,
        prophet_signs=[ProphetSigns.DEFAULT, ProphetSigns.POSITIVE, ProphetSigns.POSITIVE],
    )

    trials_config = TrialsConfig(iterations=iterations, trials=trials)

    os.makedirs(output_dir, exist_ok=True)
    robyn_model = Robyn(working_dir=output_dir)
    robyn_model.initialize(mmm_data=mmm_data, holidays_data=holidays_data, hyperparameters=hyperparameters)
    robyn_model.feature_engineering(display_plots=False, export_plots=False)

    try:
        results = robyn_model.featurized_mmm_data.modNLS['results']
        robyn_model._mm_results = {r['channel']: r for r in results}
        print(f" MM params captured: { {k: v['model_type'] for k, v in robyn_model._mm_results.items()} }")
    except Exception as e:
        print(f" MM capture failed: {e} — will use ROI anchoring for all channels")
        robyn_model._mm_results = None

    robyn_model.train_models(trials_config=trials_config, cores=cores, display_plots=False, export_plots=False)

    return robyn_model


# ════════════════════════════════════════════════════════════════════════════
# PARETO + CLUSTERING
# Unchanged — ParetoOptimizer and ClusterBuilder are Robyn internals
# They operate on Robyn model objects, not distributable data
# ════════════════════════════════════════════════════════════════════════════

def run_pareto(robyn_model, pareto_fronts, min_candidates):
    """
    Runs Pareto optimisation and clustering.
    Attaches .pareto_result and .clustered_result to model object.
    Returns robyn_model.
    """
    pareto_optimizer = ParetoOptimizer(
        mmm_data=robyn_model.mmm_data,
        model_outputs=robyn_model.model_outputs,
        hyperparameter=robyn_model.hyperparameters,
        featurized_mmm_data=robyn_model.featurized_mmm_data,
        holidays_data=robyn_model.holidays_data,
    )
    robyn_model.pareto_result = pareto_optimizer.optimize(
        pareto_fronts=pareto_fronts,
        min_candidates=min_candidates,
    )
    robyn_model.pareto_result = ParetoUtils().process_pareto_clustered_results(
        robyn_model.pareto_result, None, False
    )

    cluster_config = ClusteringConfig(
        weights=[1.0, 1.0, 1.0],
        dep_var_type=DependentVarType.REVENUE,
        cluster_by=ClusterBy.HYPERPARAMETERS,
        max_clusters=10,
        min_clusters=3,
        limit=1,
    )
    cluster_builder = ClusterBuilder(pareto_result=robyn_model.pareto_result)
    robyn_model.clustered_result = cluster_builder.cluster_models(config=cluster_config)

    top = robyn_model.clustered_result.top_solutions['sol_id'].tolist()
    

    return robyn_model


# ════════════════════════════════════════════════════════════════════════════
# SCORER
# Spark-ified where beneficial:
#   - overall_roas groupby → Spark aggregation
#   - roas_cv groupby → Spark aggregation
#   - baseline_stats groupby → Spark aggregation
#   - max_effect_share groupby → Spark aggregation
#   - carryover_data groupby → Spark aggregation
#   - max_share_delta groupby → Spark aggregation
# Kept as pandas:
#   - Hard disqualifier filtering — small subset, no benefit
#   - Normalisation — column-wise math, trivially fast in pandas
#   - Notes generation — row-wise logic, UDF would be expensive
#   - Final sort and column selection — tiny dataframe
# ════════════════════════════════════════════════════════════════════════════

def score_solutions(robyn_model, seg):
    """
    Business-first scorer with Spark-accelerated aggregations.

    Hard disqualifiers:
      1. True baseline negative in any week
      2. R² < 0.5

    Scored criteria:
      - Blended ROAS                        25%
      - DECOMP.RSSD                         18%
      - Media contribution %                12%
      - Channel ROAS CV                     12%
      - Max channel effect share dominance  10%
      - Worst spend/effect misalignment     08%
      - Carryover %                         05%
      - R² fit quality                      05%
      - NRMSE                               05%
      - Overfit penalty (R²>0.95)          -10%

    Returns scored DataFrame sorted best first.
    """
    spark = get_spark()

    pr      = robyn_model.pareto_result
    top_ids = robyn_model.clustered_result.top_solutions['sol_id'].tolist()
    hyp     = pr.result_hyp_param
    xdecomp = pr.x_decomp_agg
    xvec    = pr.x_decomp_vec_collect

    paid_channels = robyn_model.mmm_data.mmmdata_spec.paid_media_spends
    context_vars  = robyn_model.mmm_data.mmmdata_spec.context_vars or []
    organic_vars  = getattr(robyn_model.mmm_data.mmmdata_spec, 'organic_vars', []) or []

    # ── Filter to top solutions in pandas first ───────────────────────────
    # These are small slices — Spark conversion happens after filtering
    xd = xdecomp[xdecomp['sol_id'].isin(top_ids)].copy()
    xv = xvec[xvec['sol_id'].isin(top_ids)].copy()

    non_baseline        = set(paid_channels + context_vars + organic_vars + ['dep_var', 'depVarHat', 'sol_id', 'ds'])
    baseline_components = [c for c in xv.columns if c not in non_baseline]

    # ── Base metrics df — stays pandas (tiny) ────────────────────────────
    df = hyp[hyp['sol_id'].isin(top_ids)][['sol_id', 'nrmse', 'decomp.rssd', 'rsq_train']].copy()

    # ── Channel ROAS slice ────────────────────────────────────────────────
    channel_roas_pd = (
        xd[xd['rn'].isin(paid_channels)][['sol_id', 'rn', 'roi_total', 'effect_share', 'spend_share']].copy()
    )

    # ── Convert to Spark for aggregations ────────────────────────────────
    channel_roas_spark = spark.createDataFrame(channel_roas_pd)

    # ── Overall ROAS — spend-weighted average ─────────────────────────────
    # Pandas: groupby + np.average with weights
    # Spark: sum(roi * spend_share) / sum(spend_share) per sol_id
    overall_roas_spark = channel_roas_spark.groupBy('sol_id').agg(
        (F.sum(F.col('roi_total') * F.col('spend_share')) /
         (F.sum('spend_share') + F.lit(1e-10))).alias('roas')
    )
    overall_roas_pd = overall_roas_spark.toPandas()
    df = df.merge(overall_roas_pd, on='sol_id', how='left')

    # ── ROAS CV — coefficient of variation per sol_id ─────────────────────
    # Pandas: groupby + std/mean
    # Spark: stddev / (abs(mean) + epsilon)
    roas_cv_spark = channel_roas_spark.groupBy('sol_id').agg(
        (F.stddev('roi_total') /
         (F.abs(F.mean('roi_total')) + F.lit(1e-10))).alias('roas_cv')
    )
    roas_cv_pd = roas_cv_spark.toPandas()
    df = df.merge(roas_cv_pd, on='sol_id', how='left')

    # ── Baseline stats — convert xv to Spark ──────────────────────────────
    xv = xv.copy()
    xv['true_baseline']        = xv[baseline_components].sum(axis=1)
    xv['baseline_revenue_pct'] = xv['true_baseline'] / (xv['dep_var'] + 1e-10)

    xv_spark = spark.createDataFrame(
        xv[['sol_id', 'true_baseline', 'baseline_revenue_pct']]
    )

    baseline_stats_spark = xv_spark.groupBy('sol_id').agg(
        F.min('true_baseline').alias('baseline_min_weekly'),
        F.mean('true_baseline').alias('baseline_mean_weekly'),
        F.mean('baseline_revenue_pct').alias('baseline_pct_mean'),
    )
    baseline_stats_pd = baseline_stats_spark.toPandas()
    df = df.merge(baseline_stats_pd, on='sol_id', how='left')
    df['media_contribution_pct'] = 1 - df['baseline_pct_mean']

    # ── Max effect share per sol_id ───────────────────────────────────────
    max_effect_spark = channel_roas_spark.groupBy('sol_id').agg(
        F.max('effect_share').alias('max_channel_effect_share')
    )
    max_effect_pd = max_effect_spark.toPandas()
    df = df.merge(max_effect_pd, on='sol_id', how='left')

    # ── Carryover % ───────────────────────────────────────────────────────
    carryover_pd = (
        xd[xd['rn'].isin(paid_channels)][['sol_id', 'mean_carryover', 'mean_spend_adstocked']].copy()
    )
    carryover_spark = spark.createDataFrame(carryover_pd)
    carryover_agg_spark = carryover_spark.groupBy('sol_id').agg(
        F.sum('mean_carryover').alias('sum_carryover'),
        F.sum('mean_spend_adstocked').alias('sum_adstocked'),
    )
    carryover_agg_spark = carryover_agg_spark.withColumn(
        'carryover_pct',
        F.col('sum_carryover') / (F.col('sum_adstocked') + F.lit(1e-10))
    ).select('sol_id', 'carryover_pct')
    carryover_agg_pd = carryover_agg_spark.toPandas()
    df = df.merge(carryover_agg_pd, on='sol_id', how='left')

    # ── Max spend/effect share delta ──────────────────────────────────────
    share_delta_spark = channel_roas_spark.withColumn(
        'share_delta', F.abs(F.col('effect_share') - F.col('spend_share'))
    )
    max_share_delta_spark = share_delta_spark.groupBy('sol_id').agg(
        F.max('share_delta').alias('max_share_delta')
    )
    max_share_delta_pd = max_share_delta_spark.toPandas()
    df = df.merge(max_share_delta_pd, on='sol_id', how='left')

    # ── From here — stays pandas ──────────────────────────────────────────
    # Hard disqualifiers, normalisation, notes, scoring:
    # All operate on a tiny dataframe (n_top_solutions × n_metrics)
    # No benefit to Spark — overhead of createDataFrame would dominate

    viable, eliminated = df.copy(), []

    neg_baseline = viable[viable['baseline_min_weekly'] < 0]['sol_id'].tolist()
    if neg_baseline:
        
        eliminated.extend(neg_baseline)
        viable = viable[~viable['sol_id'].isin(neg_baseline)]

    low_r2 = viable[viable['rsq_train'] < 0.5]['sol_id'].tolist()
    if low_r2:
        
        eliminated.extend(low_r2)
        viable = viable[~viable['sol_id'].isin(low_r2)]

    if viable.empty:
        
        viable = df[df['baseline_min_weekly'] >= 0].copy()
        if viable.empty:
            viable = df.copy()

    notes = {sid: [] for sid in viable['sol_id'].tolist()}
    for sid in viable['sol_id']:
        row = viable[viable['sol_id'] == sid].iloc[0]
        if row['roas'] < 1:
            notes[sid].append(f"Blended ROAS={row['roas']:.2f} (<1)")
        low_ch = channel_roas_pd[
            (channel_roas_pd['sol_id'] == sid) &
            (channel_roas_pd['roi_total'] < 0.3)
        ]['rn'].tolist()
        if low_ch:
            notes[sid].append(f"Very low ROAS (<0.3): {low_ch}")
        if row.get('media_contribution_pct', 1) < 0.15:
            notes[sid].append(f"Media explains only {row['media_contribution_pct']:.0%} of revenue")
        if row.get('max_channel_effect_share', 0) > 0.5:
            top_ch = channel_roas_pd[
                channel_roas_pd['sol_id'] == sid
            ].sort_values('effect_share', ascending=False).iloc[0]['rn']
            notes[sid].append(f"Channel dominance: {top_ch} ({row['max_channel_effect_share']:.0%})")
        if row.get('carryover_pct', 0) > 0.6:
            notes[sid].append(f"High carryover ({row['carryover_pct']:.0%})")
        if row['rsq_train'] > 0.95:
            notes[sid].append(f"R²={row['rsq_train']:.3f} — check overfit")

    def norm_higher(s):
        rng = s.max() - s.min()
        return (s - s.min()) / (rng + 1e-10)

    def norm_lower(s):
        rng = s.max() - s.min()
        return 1 - (s - s.min()) / (rng + 1e-10)

    viable['roas_norm']        = viable['roas'].clip(0, 8) / 8
    viable['roas_cv_norm']     = norm_lower(viable['roas_cv'])
    viable['media_pct_norm']   = norm_higher(viable['media_contribution_pct'])
    viable['rssd_norm']        = norm_lower(viable['decomp.rssd'])
    viable['dominance_norm']   = norm_lower(viable['max_channel_effect_share'])
    viable['share_delta_norm'] = norm_lower(viable['max_share_delta'])
    viable['carryover_norm']   = norm_lower(viable['carryover_pct'])
    viable['r2_norm']          = norm_higher(viable['rsq_train'])
    viable['nrmse_norm']       = norm_lower(viable['nrmse'])
    viable['overfit_penalty']  = ((viable['rsq_train'] - 0.95) / 0.05).clip(0, 1)

    viable['score'] = (
        0.25 * viable['roas_norm']        +
        0.18 * viable['rssd_norm']        +
        0.12 * viable['media_pct_norm']   +
        0.12 * viable['roas_cv_norm']     +
        0.10 * viable['dominance_norm']   +
        0.08 * viable['share_delta_norm'] +
        0.05 * viable['carryover_norm']   +
        0.05 * viable['r2_norm']          +
        0.05 * viable['nrmse_norm']       -
        0.10 * viable['overfit_penalty']
    )

    viable = viable.sort_values('score', ascending=False)
    viable['segment']          = seg
    viable['notes']            = viable['sol_id'].map(lambda x: ' | '.join(notes.get(x, [])))
    viable['eliminated_peers'] = str(eliminated) if eliminated else 'none'

    display_cols = [
        'segment', 'sol_id', 'score',
        'roas', 'rsq_train', 'nrmse', 'decomp.rssd',
        'media_contribution_pct', 'baseline_min_weekly', 'baseline_mean_weekly',
        'carryover_pct', 'roas_cv', 'max_channel_effect_share',
        'max_share_delta', 'overfit_penalty', 'eliminated_peers', 'notes'
    ]
    return viable[[c for c in display_cols if c in viable.columns]]


# ════════════════════════════════════════════════════════════════════════════
# PARAMS EXTRACTION
# Unchanged — operates on Robyn model objects and tiny pandas slices
# All dataframes here are n_solutions × n_channels — negligibly small
# No benefit to Spark — createDataFrame overhead would dominate
# ════════════════════════════════════════════════════════════════════════════

def extract_model_params(robyn_model, seg, sol_id):
    pr      = robyn_model.pareto_result
    xdecomp = pr.x_decomp_agg
    xvec    = pr.x_decomp_vec_collect
    hyp     = pr.result_hyp_param

    paid_channels = robyn_model.mmm_data.mmmdata_spec.paid_media_spends
    paid_vars     = robyn_model.mmm_data.mmmdata_spec.paid_media_vars
    hyp_row       = hyp[hyp['sol_id'] == sol_id].iloc[0]

    fit_metrics = {
        'rsq_train':   float(hyp_row.get('rsq_train',   np.nan)),
        'nrmse':       float(hyp_row.get('nrmse',       np.nan)),
        'decomp_rssd': float(hyp_row.get('decomp.rssd', np.nan)),
        'mape':        float(hyp_row.get('mape',        np.nan)),
    }

    xd_sol      = xdecomp[(xdecomp['sol_id'] == sol_id) & (xdecomp['rn'].isin(paid_channels))].copy()
    coef_lookup = xd_sol.set_index('rn')['coef'].to_dict() if 'coef' in xd_sol.columns else {}

    hill_params = {}
    for ch in paid_channels:
        hill_params[ch] = {
            'coef':  float(coef_lookup.get(ch, np.nan)),
            'alpha': float(hyp_row.get(f'{ch}_alphas', np.nan)),
            'gamma': float(hyp_row.get(f'{ch}_gammas', np.nan)),
            'theta': float(hyp_row.get(f'{ch}_thetas', np.nan)),
        }

    channel_decomp = (
        xd_sol[['rn', 'roi_total', 'effect_share', 'spend_share', 'mean_carryover', 'mean_spend_adstocked']]
        .set_index('rn').to_dict(orient='index')
    )

    mm_params    = {}
    mm_results   = getattr(robyn_model, '_mm_results', None)
    mean_spends  = {ch: robyn_model.mmm_data.data[ch].mean() for ch in paid_channels}

    
    for spend_col, exposure_col in zip(paid_channels, paid_vars):

        if mm_results is None or exposure_col not in mm_results:
            mm_params[spend_col] = None
            print(f"    {spend_col:35s} not found — will use ROI anchoring")
            continue

        r          = mm_results[exposure_col]
        mean_spend = mean_spends[spend_col]

        if r['model_type'] == 'nls' and r['Vmax'] is not None:
            Vmax     = float(r['Vmax'])
            Km       = float(r['Km'])
            rsq      = float(r['rsq'])
            km_ratio = Km / (mean_spend + 1e-10)
            use_mm   = bool((km_ratio <= 10) and (rsq >= 0.95))

            mm_params[spend_col] = {
                'model_type':   'nls',
                'Vmax':         float(Vmax),
                'Km':           float(Km),
                'rsq':          float(rsq),
                'exposure_col': exposure_col,
                'km_ratio':     float(km_ratio),
                'use_mm':       use_mm,
            }
            flag = 'use MM' if use_mm else f'ROI anchoring (Km={km_ratio:.0f}× spend)'
            

        else:
            coef_lm = float(r['coef_lm']) if r['coef_lm'] is not None else None
            rsq     = float(r['rsq'])
            mm_params[spend_col] = {
                'model_type':   'lm',
                'coef_lm':      float(coef_lm) if coef_lm is not None else None,
                'rsq':          float(rsq),
                'exposure_col': exposure_col,
                'km_ratio':     None,
                'use_mm':       False,
            }
             

    weekly_decomp = xvec[xvec['sol_id'] == sol_id].drop(columns=['sol_id']).copy()
    if 'ds' in weekly_decomp.columns:
        weekly_decomp = weekly_decomp.rename(columns={'ds': 'date'})

    return {
        'sol_id':         sol_id,
        'segment':        seg,
        'fit_metrics':    fit_metrics,
        'hill_params':    hill_params,
        'mm_params':      mm_params,
        'channel_decomp': channel_decomp,
        'weekly_decomp':  weekly_decomp,
        'raw_data':       robyn_model.mmm_data.data.copy(),
    }


# ════════════════════════════════════════════════════════════════════════════
# MAIN SEGMENT PIPELINE
# Unchanged — orchestrates Robyn functions, all driver-side
# ════════════════════════════════════════════════════════════════════════════

def run_segment_pipeline(
    seg, df_seg, dep_var, paid_media_spends, paid_media_vars,
    context_vars, context_signs, output_dir, dt_holidays,
    prophet_country, iterations, trials, cores, adstock,
    pareto_fronts, min_candidates,
):
    """
    Full pipeline for one segment:
      Train → Pareto + Cluster → Score → Extract params for winner

    Returns:
        best_sol_id : str  (None on failure)
        scored_df   : DataFrame of all scored solutions (None on failure)
        params      : dict of extracted params for winner (None on failure)
        robyn_model : trained model object (None on train failure)
    """
    
    try:
        robyn_model = train_segment(
            df_seg=df_seg, dep_var=dep_var, paid_media_spends=paid_media_spends,
            paid_media_vars=paid_media_vars, context_vars=context_vars,
            context_signs=context_signs, output_dir=output_dir,
            dt_holidays=dt_holidays, prophet_country=prophet_country,
            iterations=iterations, trials=trials, cores=cores, adstock=adstock,
        )
        
    except Exception as e:
        
        traceback.print_exc()
        return None, None, None, None

    
    try:
        robyn_model = run_pareto(
            robyn_model=robyn_model,
            pareto_fronts=pareto_fronts,
            min_candidates=min_candidates,
        )
    except Exception as e:
       
        traceback.print_exc()
        return None, None, None, robyn_model

    
    try:
        scored_df   = score_solutions(robyn_model=robyn_model, seg=seg)
        best_sol_id = scored_df.iloc[0]['sol_id']
        
        if scored_df.iloc[0]['notes']:
            print(f"Notes: {scored_df.iloc[0]['notes']}")
    except Exception as e:
        print(f" Scoring failed: {e}")
        traceback.print_exc()
        return None, None, None, robyn_model

    print(f"\n  [PARAMS] {seg} → {best_sol_id}")
    try:
        params = extract_model_params(robyn_model=robyn_model, seg=seg, sol_id=best_sol_id)
        print(f" Params extracted")
    except Exception as e:
        print(f"Param extraction failed: {e}")
        traceback.print_exc()
        params = None

    return best_sol_id, scored_df, params, robyn_model