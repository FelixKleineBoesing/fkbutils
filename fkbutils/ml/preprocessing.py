import numpy as np
import pandas as pd


def sampling(data, label, method: str = "up"):
    """

    :param data:
    :param label:
    :param method:
    :return:
    """
    pd.set_option('chained_assignment', None)
    assert method in ["up", "down"]
    data["class"] = label
    if method == "up":
        factor = -1
    else:
        factor = 1
    class_neg = data.loc[data["class"] == 0, :]
    class_pos = data.loc[data["class"] == 1, :]

    count_neg = class_neg.shape[0]
    count_pos = class_pos.shape[0]
    args = {}
    if factor * count_neg < factor * count_pos:
        if count_neg > count_pos:
            args["replace"] = True
        down_sampled_df = class_pos.sample(count_neg, **args)
        sampled_data = pd.concat((down_sampled_df, class_neg))
    else:
        if count_pos > count_neg:
            args["replace"] = True
        down_sampled_df = class_neg.sample(count_pos, replace=True)
        sampled_data = pd.concat((down_sampled_df, class_pos))
    sampled_data.reset_index(inplace=True, drop=True)
    sampled_data = sampled_data.iloc[np.random.permutation(sampled_data.shape[0]),:]
    sampled_data.reset_index(inplace=True, drop=True)
    sampled_label = sampled_data.pop("class")
    return sampled_data, sampled_label