# -*- coding: utf-8 -*-

import datetime
import glob
import os


def new_output_dir(root):
    """
    Produce the name of a fresh output directory.

    Args:
        root: str, the base directory which may contain previously used output
            directories

    Returns:
        str, A fresh output directory name, such as <root>/2019-05-31#004 where
        004 indicates 3 previous runs exist.
    """
    today = datetime.date.today()
    files_glob = (os.path.join(root, "{}*".format(today.isoformat())))
    todays_outputs = sorted(glob.glob(files_glob), reverse=True)

    if todays_outputs == []:
        return os.path.join(root, "%s#%03d" % (today.isoformat(), 1))

    previous = int(todays_outputs[0][-3:])
    return os.path.join(root, "%s#%03d" % (today.isoformat(), previous + 1))
