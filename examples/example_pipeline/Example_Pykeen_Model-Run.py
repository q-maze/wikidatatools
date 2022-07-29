# This file is for running the models is a PyKeen optimization pipeline

import pickle
import torch
import random

from pykeen.datasets import EagerDataset
from pykeen.hpo import hpo_pipeline


def main(): 
    # clear gpu cashe
    torch.cuda.empty_cache()
    
    # EagerDataSet Path
    ds_path = '/subsets/humans/human_subset'
    
    # Models used in the paper
    # models = ["TransE", "ConvE", "ComplEx", "DistMult", "ConvKB", "PairRE", "HolE", "RESCAL", "TorusE"]
    model_type = "ComplEx"
    
    # For slurm jobs, to show what i
    print(model_type)
    dataset = EagerDataset.from_directory_binary(ds_path)
    
    # training pipeline
    result_hpo = hpo_pipeline(
        device='gpu',
        n_trials=100,
        dataset=dataset,
        model=model_type,
        model_kwargs=dict(random_seed=3142), # Setting seed
        training_loop='sLCWA',
        training_kwargs=dict(
            use_tqdm_batch=False,
            batch_size=128),
        # Increase the max for larger datasets
        training_kwargs_ranges=dict(
            num_epochs=dict(type=int, low=100, high=600, step=100)),
        loss='SoftplusLoss',
        negative_sampler='basic',
        optimizer='adam',
        optimizer_kwargs_ranges=dict(
            lr=dict(type=int, low=0.01, high=0.1, step=0.005)),
        evaluator='RankBasedEvaluator',
        evaluator_kwargs=dict(batch_size=32),
        evaluation_kwargs=dict(batch_size=32),
        stopper='early',
        # Increase the frequency for larger datasets
        stopper_kwargs=dict(frequency=50, patience=2, relative_delta=0.01)
    )
    
    # Save results of the pipeline for review
    result_hpo.save_to_directory('hpo_results/'+model_type)
    
if __name__=='__main__':
    main()