"""Analyze scrapped molecular dynamics datasets and files."""
# Standard library imports

# Third party imports
import matplotlib.pyplot as plt
import pandas as pd
import requests
import seaborn as sns


def get_cli_arguments():
    """Argument parser.

    This function parses the name of the dataset and files input files.

    Returns
    -------
    str
        Name of the tsv input files.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "input_dataset_file", metavar="input_file", type=str, help="Input dataset tsv file."
    )
    parser.add_argument(
        "input_files_file", metavar="input_file", type=str, help="Input files tsv file."
    )
    return parser.parse_args()


def read_df(dataset_tsv, files_tsv):
    """tsv reader.

    This function reads the dataset and files input files into a pandas dataframe.

    Parameters
    ----------
    dataset_tsv : str
        The name of the dataset file.
    files_tsv : str
        The name of the files file.

    Returns
    -------
    datasets_df: pandas dataframe
        Contains the dataset content.
    files_df: pandas dataframe
        Contains the files content.
    """
    datasets_df = pd.read_csv(dataset_tsv, sep="\t")
    files_df = pd.read_csv(files_tsv, sep="\t")
    return datasets_df, files_df


def prepare_timeline_df(df):
    """file and dataset counter.

    This function counts the number of datasets, files and unique authors per year.

    Parameters
    ----------
    df : pandas dataframe
        Takes the datasets dataframe.

    Returns
    -------
    tmp_df: pandas dataframe
        Contains a reduced dataframe with only counts per year.
    """
    tmp_df = df.copy()
    tmp_df['year'] = tmp_df['date_creation'].apply(lambda x: int(x.split('-')[0]))
    tmp_df_tmp = tmp_df.copy()
    tmp_df_tmp['datasets'] = 1
    tmp_df_tmp = pd.pivot_table(tmp_df_tmp, aggfunc='sum', values='datasets', index='year').reset_index(drop=False)
    tmp_df_tmp['type'] = 'datasets'
    tmp_df_tmp2 = tmp_df.copy()
    tmp_df_tmp2 = pd.pivot_table(tmp_df_tmp2, aggfunc='sum', values='file_number', index='year').reset_index(drop=False)
    tmp_df_tmp2['type'] = 'files'
    tmp_df_tmp3 = tmp_df.copy()
    tmp_df_tmp3['author_count'] = 1
    tmp_df_tmp3 = tmp_df_tmp3.drop_duplicates(subset=['author'])
    tmp_df_tmp3 = pd.pivot_table(tmp_df_tmp3, aggfunc='sum', values='author_count', index='year').reset_index(drop=False)
    tmp_df_tmp3['type'] = 'authors'
    tmp_df = pd.concat([tmp_df_tmp, tmp_df_tmp2, tmp_df_tmp3])
    return tmp_df


def plot_timeline_dataset_files(df):
    """plotter timeline vs dataset and files

    This function plots the number of datasets and files per year.

    Parameters
    ----------
    df : pandas dataframe
        Takes the timeline prepared dataframe.
    """
    df = df.loc[df['type']!='authors']
    fig, ax1 = plt.subplots(figsize=(9, 6))
    sns.set_style("white")
    sns.set_style("ticks")
    ax2 = ax1.twinx()
    palette = {c: "b" if c != "datasets" else "r" for c in df["type"].unique()}
    ax = sns.barplot(ax = ax1, x="year", y="datasets", hue='type', data=df, palette = palette)
    ax.legend([])
    ax = sns.barplot(ax = ax2, x="year", y="file_number", hue='type', data=df, palette=palette)
    plt.title('Dataset and total file counts per year')
    plt.savefig("timeline_dataset_files.svg",dpi=350)
    plt.show()


def plot_timeline_dataset_authors(df):
    """plotter timeline vs dataset and authors

    This function plots the number of datasets and unique authors per year.

    Parameters
    ----------
    df : pandas dataframe
        Takes the timeline prepared dataframe.
    """
    df = df.loc[df['type']!='files']
    fig, ax1 = plt.subplots(figsize=(9, 6))
    sns.set_style("white")
    sns.set_style("ticks")
    palette = {c: "grey" if c != "datasets" else "r" for c in df["type"].unique()}
    ax = sns.barplot(x="year", y="datasets", hue='type', data=df, palette = palette, hue_order=['datasets', 'authors'])
    ax = sns.barplot(x="year", y="author_count", hue='type', data=df, palette=palette, hue_order=['datasets', 'authors'])
    ax.set_ylabel('Counts')
    leg = plt.legend(labels=['datasets', 'author_count'])
    LH = leg.legendHandles
    LH[0].set_color('r')
    LH[1].set_color('grey')
    plt.title('Dataset and unique author counts per year')
    plt.savefig("timeline_dataset_authors.svg", dpi=350)
    plt.show()


def plot_origin_count(df):
    """plotter temperature count

    This function plots the number of mdp files by their temperature.

    Parameters
    ----------
    df : pandas dataframe
        Takes the mdp prepared dataframe.
    """
    fig, ax1 = plt.subplots(figsize=(9, 6))
    sns.set_style("white")
    sns.set_style("ticks")
    ax = sns.countplot(ax = ax1, x='year', hue='origin', data=df)
    plt.title(f'File count for each year by origin')
    plt.savefig("origin_timeline_count.svg", dpi=350)
    plt.show()
    
    
def prepare_ext_count_df(df):
    """file extension grouper.

    This function generates new groups by file extensions.

    Parameters
    ----------
    df : pandas dataframe
        Takes the merged datasets and files dataframe.

    Returns
    -------
    count_article_df: pandas dataframe
        Contains a dataframe with the groups from the file extensions.
    """
    coordinate = ["tpr","gro","psf","crd","coor","namdbin","coord", 'pdb']
    topology = ["mdp","itp","ndx","top","cpt","namd","inp","prm","ntf","xsc","prmtop","top"]
    trajectory = ["xtc","trr","edr","dcd","vel","prm7","crdbox","inpcrd","mdcrd","nc","ncdf","trj"]
    gromacs = ["tpr","gro","mdp","itp","ndx","top","xtc","trr","edr","cpt"]
    namd = ["psf","namd","inp","prm","ntf","crd","dcd","coor","namdbin","vel","xsc"]
    amber = ["prmtop","coord","prm7","top","crdbox","inpcrd","mdcrd","nc","ncdf","trj"]
    def ext_cat(ext):
        try:
            if ext.lower() in coordinate:
                return 'coordinate'
            elif ext.lower() in topology:
                return 'topology'
            elif ext.lower() in trajectory:
                return 'trajectory'
            else:
                return 'other'
        except:
            return 'other'
    def engine(ext):
        try:
            if ext.lower() in gromacs:
                return 'gromacs'
            elif ext.lower() in namd:
                return 'namd'
            elif ext.lower() in amber:
                return 'amber'
            else:
                return 'other'
        except:
            return 'other'
        
    count_article_df = []
    for index, article in df.iterrows():
        date = article['date_creation']
        year = date.split('-')[0]
        try:
            author = article['author']
        except:
            author = 0
        file_ext = article['file_type']
        file_size = article['file_size']
        count_article_df.append([year, author, file_ext, ext_cat(file_ext), engine(file_ext), file_size, article['dataset_id']])


    count_article_df = pd.DataFrame(data=count_article_df, columns=['year', 'author', 'ext', 'cat', 'engine', 'size', 'dataset id'])
    count_article_df = count_article_df.sort_values(by=['year']).reset_index(drop=True)
    return count_article_df


def plot_timeline_category(df):
    """plotter timeline vs category

    This function plots the number of files in each category per year.

    Parameters
    ----------
    df : pandas dataframe
        Takes the grouped prepared dataframe.
    """
    df_tmp = df.copy()
    df_tmp = df_tmp.loc[df_tmp['cat']!='other']
    fig, ax1 = plt.subplots(figsize=(9, 6))
    sns.set_style("white")
    sns.set_style("ticks")
    ax = sns.countplot(ax = ax1, x='year', hue="cat", data=df_tmp)
    plt.title('File count per year for each category')
    plt.savefig("timeline_category.svg", dpi=350)
    plt.show()


def plot_timeline_engine(df):
    """plotter timeline vs engine

    This function plots the number of files in each engine per year.

    Parameters
    ----------
    df : pandas dataframe
        Takes the grouped prepared dataframe.
    """
    df_tmp = df.copy()
    df_tmp = df_tmp.loc[df_tmp['engine']!='other']
    fig, ax1 = plt.subplots(figsize=(9, 6))
    sns.set_style("white")
    sns.set_style("ticks")
    ax = sns.countplot(ax = ax1, x='year', hue="engine", data=df_tmp)
    plt.title('File count per year for each engine')
    plt.savefig("timeline_engine.svg", dpi=350)
    plt.show()


def plot_timeline_size_engine(df):
    """plotter timeline vs filesize per engine

    This function plots the mean filesize in each engine per year.

    Parameters
    ----------
    df : pandas dataframe
        Takes the grouped prepared dataframe.
    """
    df_tmp = df.copy()
    df_tmp = df_tmp.loc[df_tmp['cat']=='trajectory']
    fig, ax1 = plt.subplots(figsize=(9, 6))
    sns.set_style("white")
    sns.set_style("ticks")
    ax = sns.barplot(ax = ax1, x='year', y='size', hue="engine", data=df_tmp)
    plt.title('Mean file size per year for each engine')
    plt.savefig("timeline_size_engine.svg", dpi=350)
    plt.show()


def plot_extension_engine(df):
    """plotter extension vs engine

    This function plots the number of files of a specific extension per engine.

    Parameters
    ----------
    df : pandas dataframe
        Takes the grouped prepared dataframe.
    """
    df_tmp = df.copy()
    df_tmp = df_tmp.loc[df_tmp['engine']!='other']
    fig, ax1 = plt.subplots(figsize=(9, 6))
    sns.set_style("white")
    sns.set_style("ticks")
    ax = sns.countplot(ax = ax1, x='ext', hue="engine", data=df_tmp)
    plt.title('File extension count for each engine')
    plt.savefig("extension_engine.svg", dpi=350)
    plt.show()


def get_info_from_mdp(df):
    """mdp info grapper.

    This function graps the info from mdp files (currently only temperature)

    Parameters
    ----------
    df : pandas dataframe
        Takes the merged datasets and files dataframe or just the files dataframe.

    Returns
    -------
    temperatures: pandas dataframe
        Contains a dataframe with the temperatures for each file.
    """
    mdp_files_df = df.copy()
    mdp_files_df = mdp_files_df.loc[mdp_files_df['file_type']=='mdp'].reset_index(drop=True)
    print(f'Number of mdp files: {len(mdp_files_df)}')
    
    temperatures = []
    for index, file in mdp_files_df.iterrows():
        link = file['file_url']
        f = requests.get(link)
        for line in f:
            line = line.decode('UTF-8')
            if line.find('ref_t') != -1:
                temp = line.split('ref_t')[1].strip().split('\n')[0].split('=')[1].strip().split(' ')[0]
                temperatures.append(temp)
                break
    temperatures = pd.DataFrame(data=temperatures, columns=['temperatures'])
    temperatures = temperatures.sort_values(by=['temperatures']).reset_index(drop=True)
    return temperatures


def plot_temp_count(df):
    """plotter temperature count

    This function plots the number of mdp files by their temperature.

    Parameters
    ----------
    df : pandas dataframe
        Takes the mdp prepared dataframe.
    """
    fig, ax1 = plt.subplots(figsize=(9, 6))
    sns.set_style("white")
    sns.set_style("ticks")
    ax = sns.countplot(ax = ax1, x='temperatures', data=df)
    plt.title(f'File count for each temperature extracted from {len(df)} mdp files')
    plt.savefig("temp_count.svg", dpi=350)
    plt.show()


def get_info_from_gro(df):
    """gro info grapper.

    This function graps the info from gro files (currently only system size)

    Parameters
    ----------
    df : pandas dataframe
        Takes the merged datasets and files dataframe or just the files dataframe.

    Returns
    -------
    system_count_df: pandas dataframe
        Contains a dataframe with the number of atoms for each file.
    """
    gro_files_df = df.copy()
    gro_files_df = gro_files_df.loc[gro_files_df['file_type']=='gro'].reset_index(drop=True)
    print(f'Number of gro files: {len(gro_files_df)}')

    system_count_df = []
    for index, file in gro_files_df.iterrows():
        link = file['file_url']
        try:
            f = requests.get(link)
            for index, line in enumerate(f):
                line = line.decode('UTF-8')
                if index == 0:
                    size = int(line.split('\n')[1].strip())
                    system_count_df.append(size)
                    break
        except:
            print(f"ERROR for dataset ID: {file['dataset_id']}, file name: {file['file_name']}")

    system_count_df = pd.DataFrame(data=system_count_df, columns=['atoms'])
    system_count_df = system_count_df.sort_values(by=['atoms']).reset_index(drop=True)
    return system_count_df


def plot_sys_size_count(df):
    """plotter system size count

    This function plots the number of gro files which system lies in a certain size range.

    Parameters
    ----------
    df : pandas dataframe
        Takes the gro prepared dataframe.
    """
    df_tmp = df.copy()
    df_tmp.loc[df_tmp['atoms']<=1000] = 1000
    df_tmp.loc[(df_tmp['atoms']>1000) & (df_tmp['atoms']<=10000)] = 10000
    df_tmp.loc[(df_tmp['atoms']>10000)] = 100000
    df_tmp.loc[df_tmp['atoms']==1000] = '<= 1000'
    df_tmp.loc[df_tmp['atoms']==10000] = '1000 > and > 10000'
    df_tmp.loc[df_tmp['atoms']==100000] = '> 100000'
    fig, ax1 = plt.subplots(figsize=(10, 6))
    sns.set_style("white")
    sns.set_style("ticks")
    ax = sns.countplot(ax = ax1, x='atoms', data=df_tmp)
    plt.title(f'File counts for each size (= number of atoms) extracted from {len(df)} gro files')
    plt.savefig("sys_size_count.svg", dpi=350)
    plt.show()


def prep_analyze(arg):
    """
    Main prep function.
    """
    # read dataframe from datasets and files
    dataset_tsv = arg.input_dataset_file
    files_tsv = arg.input_files_file
    raw_datasets_df, raw_files_df = read_df(dataset_tsv, files_tsv)
    print(f'Number of files: {len(raw_files_df)}\nNumber of datasets: {len(raw_datasets_df)}')

    # cleaning the data from duplicates
    datasets_df = raw_datasets_df.copy()
    datasets_df = datasets_df.drop_duplicates(subset=['dataset_id', 'origin', 'doi', 'date_creation', 'date_last_modified']).reset_index(drop=True)
    files_df = raw_files_df.copy()
    files_df = files_df.drop_duplicates().reset_index(drop=True)
    print(f'Number of files after cleaning: {len(files_df)}\nNumber of datasets: {len(datasets_df)}')

    # combine datasets and files dataframe
    all_df = pd.merge(datasets_df, files_df, on=['dataset_id', 'origin'], how='right')
    #print(all_df.columns)

    
    return datasets_df, files_df, all_df


if __name__ == "__main__":
    # Parse input arguments
    arg = get_cli_arguments()
    
    # Call extract main prep function
    datasets_df, files_df, all_df = prep_analyze(arg)
    
    # timeline plot
    timeline_ana_df = prepare_timeline_df(datasets_df)
    plot_timeline_dataset_files(timeline_ana_df)
    plot_timeline_dataset_authors(timeline_ana_df)

    # plot file-dependent timeline plots
    count_article_df = prepare_ext_count_df(all_df)
    plot_timeline_category(count_article_df)
    plot_timeline_engine(count_article_df)
    plot_timeline_size_engine(count_article_df)
    plot_extension_engine(count_article_df)

    # get mdp information
    temp_df = get_info_from_mdp(all_df)
    # plot mdp information
    plot_temp_count(temp_df)

    # get mdp information
    sys_size_df = get_info_from_gro(all_df)
    # plot information
    plot_sys_size_count(sys_size_df)
