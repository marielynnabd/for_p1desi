""" Functions to read DESI delta files (Y1), and to filter them for data split tests """

import numpy as np
import os, sys, glob
import multiprocessing
from multiprocessing import Pool
import fitsio


def filter_delta_singlefile(delta_file_in_name, outdir, filter_with_qso_tid, qso_tid=None, arg_name=None, arg_value=None, selection_criterion=None):
    """ This function:
    - filters the delta_file based on a certain criterion (either matching qso_tid, or selecting hdus with conditions on header args ['MEANSNR'/others...] below/above/equal arg_value)
    - writes the new filtered delta_file in outdir

    Arguments:
    ----------
    delta_file_in_name: String
    delta fits file, DESI Y1 format

    outdir: String
    Output directory of Deltas
    
    filter_with_qso_tid: Boolean
    If we want to filter deltas according to a list of qso_tid or not
    
    qso_tid: Array of floats
    Array containing all qso target id for which we want to keep deltas. This must be given if filter_with_qso_tid
    
    arg_name: String
    Name of the argument in deltas header, for example 'MEANSNR' (PS: arg_name can only be in header)
    
    arg_value: Float
    Value of argument we want to select below/above/equal
    
    selection_criterion: String, Options: 'below', 'above', 'equal' (PS: option above is >=, but option below is just <)
    The selection criterion with respect to arg_value

    Return:
    -------
    delta_file_out: Fits file
    Similar to delta_file_in but filtered
    
    """
    
    # Creating output file
    output_name = os.path.basename(delta_file_in_name)
    output_delta_file_name = os.path.join(outdir, output_name)
    output_delta_file = fitsio.FITS(output_delta_file_name, 'rw', clobber=True)

    # Opening delta_file
    delta_file_in = fitsio.FITS(delta_file_in_name)
    n_hdu = len(delta_file_in)-1 # Each delta file contains many hdu (don't take into account HDU0)

    # Looping over hdus
    for hdu in delta_file_in[1:]:
        delta_header = hdu.read_header()
        delta_ID = delta_header['TARGETID']
        
        if filter_with_qso_tid:
            if qso_tid is not None:
                if delta_ID in qso_tid:
                    # Read data and header from the current HDU
                    delta_data = hdu.read()
                    # Append the selected HDU to the new FITS file
                    output_delta_file.write(delta_data, header=delta_header, extname=str(delta_ID))
            else:
                print('Warning: Option filter_with_qso_tid but no qso_tid is given in input, therefore the output file will be empty, i.e. no selected deltas')
        else:
            if all (x is not None for x in [arg_name, arg_value, selection_criterion]):
                hdu_arg_value = delta_header[arg_name]
                if selection_criterion == 'equal':
                    if hdu_arg_value == arg_value:
                        # Read data and header from the current HDU
                        delta_data = hdu.read()
                        # Append the selected HDU to the new FITS file
                        output_delta_file.write(delta_data, header=delta_header, extname=str(delta_ID))
                elif selection_criterion == 'below':
                    if hdu_arg_value < arg_value:
                        # Read data and header from the current HDU
                        delta_data = hdu.read()
                        # Append the selected HDU to the new FITS file
                        output_delta_file.write(delta_data, header=delta_header, extname=str(delta_ID))
                elif selection_criterion == 'above':
                    if hdu_arg_value >= arg_value:
                        # Read data and header from the current HDU
                        delta_data = hdu.read()
                        # Append the selected HDU to the new FITS file
                        output_delta_file.write(delta_data, header=delta_header, extname=str(delta_ID))
            else:
                print('Warning: one of arg_name/arg_value/selection_criterion is None, therefore the output file will be empty, i.e. no selected deltas')
                        

    # Closing output file
    output_delta_file.close()
            
    # Closing delta_file
    delta_file_in.close()


def filter_deltas_in_dir(deltas_dir, outdir, filter_with_qso_tid, qso_tid=None, arg_name=None, arg_value=None, selection_criterion=None, ncpu='all'):
    """ This function parallelizes filter_delta_singlefile over all delta files in deltas_dir

    Arguments:
    ----------
    deltas_dir: string
    Directory of picca deltas from full sample
    
    outdir: String
    Output directory of Deltas

    ncpu: int or 'all'
    For multiprocessing.Pool

    Return:
    -------
    delta_file_out corresponding to each delta_file_in in deltas_dir
    
    """

    searchstr = '*'
    deltafiles = glob.glob(os.path.join(deltas_dir, f"delta{searchstr}.fits.gz"))

    if ncpu=='all':
        ncpu = multiprocessing.cpu_count()

    print("Nb of delta files:", len(deltafiles))
    print("Number of cpus:", multiprocessing.cpu_count())

    with Pool(ncpu) as pool:
        output_filter_delta_singlefile = pool.starmap(
            filter_delta_singlefile,
            [[f, outdir, filter_with_qso_tid, qso_tid, arg_name, arg_value, selection_criterion] for f in deltafiles]
        )

