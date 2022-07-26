# -*- coding: utf-8 -*-

import datetime


def new_output_dir(root):
    """
    Produce the name of a fresh output directory.

    Args:
        root (Path): the base directory which may contain previously used output
            directories

    Returns:
        str, A fresh output directory name, such as <root>/2019-05-31#004 where
        004 indicates 3 previous runs exist.
    """
    today = datetime.date.today()
    fp = root.glob("%s*" % today.isoformat())
    todays_outputs = sorted(fp, reverse=True)

    if todays_outputs == []:
        return root / ("%s#%03d" % (today.isoformat(), 1))

    previous_name = str(todays_outputs[0])
    previous_number = int(previous_name[-3:])
    return root / ("%s#%03d" % (today.isoformat(), previous_number + 1))
