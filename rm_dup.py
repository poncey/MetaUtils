import pandas as pd
import tqdm
import sys, os


def remove_dup_df(df):
    df = df.dropna(axis=0, how='any')
    dup_inds = df[df['GeneName'].duplicated()]['GeneName'].drop_duplicates().to_list() # duplicated column indexes
    df_nondup = df.drop_duplicates(subset='GeneName', keep=False) # undup dataframe
    df_dup = []
    for i in tqdm.tqdm(dup_inds):
        dupitems = df[df['GeneName'] == i]
        dup_notz = (dupitems.iloc[:, 1:].sum(axis=1) != 0).to_list()
        # find items that contains value != 0
        if True in dup_notz:
            df_dup.append(pd.DataFrame(pd.concat([pd.Series({'GeneName':i}),dupitems[dup_notz].mean(axis=0)])).T)
        else:
            df_dup.append(pd.DataFrame(dupitems.iloc[0]).T)
    df_dup = pd.concat(df_dup, axis=0)
    df_final = pd.concat([df_dup, df_nondup], axis=0)
    assert True not in df_final.duplicated(subset='GeneName').to_list()
    return df_final, df_dup


if __name__ == "__main__":
    tqdm.tqdm.write("Reading excel data...", end='')
    xl = pd.ExcelFile(sys.argv[1])
    dir_name = "results_{:s}".format(sys.argv[1][:-5])
    if not os.path.exists(dir_name):
        os.mkdir(dir_name)
    tqdm.tqdm.write("Reading Complete!")
    for s_name in xl.sheet_names:
        os.mkdir(os.path.join(dir_name, s_name))
        tqdm.tqdm.write("Processing duplicates for {:s}".format(s_name))

        dataframe = xl.parse(s_name)
        dataframe_final, dataframe_dup = remove_dup_df(dataframe)

        dataframe_final.to_csv(os.path.join(os.path.join(dir_name, s_name), "clean_data.csv"), index=False)
        dataframe_dup.to_csv(os.path.join(os.path.join(dir_name, s_name), "dup_data.csv"), index=False)
        tqdm.tqdm.write("Saving Files Complete!")
        
