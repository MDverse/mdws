import pathlib

def verify_output_directory(directory):
    """Verify output directory exists.

    Create it if necessary.

    Parameters
    ----------
    directory : str
        Path to directory to store results
    """
    directory_path = pathlib.Path(directory)
    if directory_path.is_file():
        raise FileNotFoundError(f"{directory} is an existing file.")
    if directory_path.is_dir():
        print(f"Output directory {directory} already exists.")
    else:
        directory_path.mkdir(parents=True, exist_ok=True)
        print(f"Created output directory {directory}")