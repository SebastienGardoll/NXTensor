#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Oct 29 10:41:15 2020

@author: Sébastien Gardoll
"""

import numpy as np

from typing import Tuple, Sequence

from matplotlib import pyplot as plt


def display_channels(tensor: np.ndarray, variable_names: Sequence, title: str, is_channels_last: bool,
                     plot_size: Tuple[float, float] = (20, 2.5), has_to_invert_latitudes: bool = False) -> None:
    if is_channels_last:
      # Brings back the channels from the last dimension to the first.
      tensor = tensor.swapaxes(0,2).swapaxes(1, 2)
    if has_to_invert_latitudes:
      tensor = tensor.swapaxes(1, 2)
    plt.figure(figsize=plot_size)
    nb_channel = tensor.shape[0]
    for channel_id in range(0, nb_channel):
        channel = tensor[channel_id]
        plt.subplot(1, nb_channel, (channel_id+1))
        plt.title(variable_names[channel_id], {'fontsize': 14})
        # Remove ticks from the x and y axes
        plt.xticks([])
        plt.yticks([])
        plt.imshow(channel, cmap='gist_rainbow_r', interpolation="none")
    plt.suptitle(title, fontsize=16, va='bottom')
    plt.show()
