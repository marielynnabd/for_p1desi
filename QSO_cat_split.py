""" Functions to read DESI qso_cat files, and to filter them for data split tests based on a list of wanted qso_tid"""

import numpy as np
import os, sys, glob
import multiprocessing
from multiprocessing import Pool
import fitsio


def qso_is_in_tid(oneqso_info_from_qso_cat_in, qso_tid):
    """ This function:
    - checks if qso targetid is in qso_tid
    - returns 1 if selected, 0 otherwise

    Arguments:
    ----------
    oneqso_info_from_qso_cat_in: Fits file
    qso info to check if it should be selected or not

    qso_tid: Array of floats
    Array containing all qso target id we want to keep

    Return:
    -------
    select_qso_nb: number: 0 or 1
    1 if qso should be selected, 0 if it shouldn't
    """

    qso_TARGETID = oneqso_info_from_qso_cat_in['TARGETID']

    if qso_TARGETID in qso_tid:
        return 1
    else:
        return 0


def filter_qso_cat(qso_cat_in_name, qso_tid, ncpu='all'):
    """ This function:
    - filters the qso_cat_in matching qso_tid by parallelizing over all quasars in qso_cat_in
    - returns the new filtered qso_cat_out

    Arguments:
    ----------
    qso_cat_in_name: String
    Path to qso catalog to be filtered

    qso_tid: Array of floats
    Array containing all qso target id we want to keep

    ncpu: int or 'all'
    For multiprocessing.Pool

    Return:
    -------
    qso_cat_out: Fits file
    Similar to qso_cat_in but filtered
    """

    # Reading qso_cat_in
    qso_cat_in_file = fitsio.FITS(qso_cat_in_name)
    qso_cat_in = qso_cat_in_file[1].read() # First hdu and only one
    n_qso = len(qso_cat_in)

    if ncpu=='all':
        ncpu = multiprocessing.cpu_count()
    
    # Parallelizing over all qso in input catalog
    with Pool(ncpu) as pool:
        output_qso_is_in_tid = pool.starmap(
            qso_is_in_tid,
            [[qso_cat_in[i_qso], qso_tid] for i_qso in range(n_qso)]
        )
    
    output_qso_is_in_tid = [x for x in output_qso_is_in_tid if x is not None]
    select_qso = np.array([])
    select_qso = np.append(select_qso, [output_qso_is_in_tid[i] for i in range(len(output_qso_is_in_tid))])
    qso_cat_out = qso_cat_in[(select_qso == 1)]
    
    return qso_cat_out

