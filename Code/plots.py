import os
import numpy as np
import matplotlib.pyplot as plt


IMAGES_PATH = os.path.join(os.path.join(os.path.dirname(os.getcwd()),"Data/"), "static/")
if not os.path.exists(IMAGES_PATH):
        os.makedirs(IMAGES_PATH)

def save_fig(fig_id, tight_layout=True, fig_extension="png", resolution=300):
    path = os.path.join(IMAGES_PATH, fig_id + "." + fig_extension)
    print("Saving figure", fig_id)
    if tight_layout:
        plt.tight_layout()
    plt.savefig(path, format=fig_extension, dpi=resolution)


def images(target_rows,ref_rows,fam,num):
    """
    Takes the target and reference tuples
    :param target_rows:
    :param ref_rows:
    :return:
    """
    f, a, m = fam
    target_data = dict(target_rows)
    ref_data = dict(ref_rows)

    n_groups = len(target_data)

    means_frank = [float(i) for i in target_data.values()]
    means_guido = [float(i) for i in ref_data.values()]

    # create plot
    fig, ax = plt.subplots()
    index = np.arange(n_groups)
    bar_width = 0.35
    opacity = 0.8

    rects1 = plt.bar(index, means_frank, bar_width,
                     alpha=opacity,
                     color='b',
                     label='Married')

    rects2 = plt.bar(index + bar_width, means_guido, bar_width,
                     alpha=opacity,
                     color='g',
                     label='Unmarried')

    plt.xlabel(a)
    plt.ylabel('{}({})'.format(f,m))
    plt.title('{} by {} for married and unmarried.'.format(m,a))
    plt.xticks(index + bar_width, target_data.keys(),rotation=45,horizontalalignment="right")
    plt.legend()

    plt.tight_layout()
    #plt.show()
    save_fig(str(num))