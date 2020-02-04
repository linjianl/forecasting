import numpy as np
import random

def ts_train_test_split(df, train_length, pred_size, cv_size = 10, random_seed = 42):
    '''
    ts_train_test_split returns a list of (train, test) indices of custom train and test length that can be used
    in cross-validation for hyper-parameter optimization. If cv_size is provided it will randomly sample the cutoff
    points and train on the period of size train_length immidiately proceeding them.
    ------------------------------------------------------------------------------------------------------
    Input:
        df: pandas df.
            Training time series. Used to compute the length to use for the generation of the cutoff indices.
        train_length: int.
            Number of days to be used for training.
        pred_size: int.
            Number of days to forecast.
        cv_size: int, optional, default = 10.
            Number of train/test splits to use for the cross-validation.
            Set to None for all the cutoff points to be used.
        random_seed: int, optional, default = 42.
            seed for the random sampler used to select cutoffs when cv_size != None.

    Output: list of tuples.
        [(train indices 1, test indices 1), ..., (train indices n, test indices n)]
    '''

    df_size = df.shape[0]
    df_ind = np.arange(df_size)
    # Generate equally spaced (pred_size) cutoff indices starting at pred_size before series size.
    cutoffs = reversed(range(df_size, train_length, -pred_size)[1:])
    # sampling cv_size cutoff indices from the available set.
    if cv_size:
        random.seed = random_seed
        cutoffs = random.sample(list(cutoffs), cv_size)
    indices = []
    # generating training and test indices using the cutoffs.
    for cutoff in cutoffs:
        test_ind = df_ind[cutoff:cutoff+pred_size]
        train_ind = df_ind[cutoff-train_length:cutoff]
        indices.append((train_ind,test_ind))
    return indices

def ts_train_test_backtest_split(df, train_length, pred_size):
    '''
    ts_train_test_backtest_split returns a list of (train, test) indices of custom train and test length that can be used
    in back testing cross-validation for hyper-parameter optimization.
    ------------------------------------------------------------------------------------------------------
    Input:
        df: pandas df.
            Training time series. Used to compute the length to use for the generation of the cutoff indices.
        train_length: int.
            minimum number of days to be used for training.
        pred_size: int.
            Number of days to forecast.

    Output: list of tuples.
        [(train indices 1, test indices 1), ..., (train indices n, test indices n)]
    '''

    df_size = df.shape[0]
    df_ind = np.arange(df_size)
    # Generate equally spaced (pred_size) cutoff indices starting at pred_size before series size.
    cutoffs = reversed(range(df_size, train_length, -pred_size)[1:])
    indices = []
    # generating training and test indices using the cutoffs.
    for cutoff in cutoffs:
        test_ind = df_ind[cutoff:cutoff+pred_size]
        train_ind = df_ind[0:cutoff]
        indices.append((train_ind,test_ind))
    return indices
