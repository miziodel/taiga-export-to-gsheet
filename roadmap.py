import pandas as pd
import io
import requests
from datetime import datetime

import gspread
from oauth2client.service_account import ServiceAccountCredentials


taiga_project_slug = 'mir'
related_us_prefix = taiga_project_slug + '#'
epics_url = 'https://taiga.20tab.com/api/v1/epics/csv?uuid=680af739cd604a1fb5e0'
epics_columns_to_keep = ['ref','subject','status',
                         'tags','related_user_stories',
                         'year','quarter','outcome']
stories_url = 'https://taiga.20tab.com/api/v1/userstories/csv?uuid=80a333675fef469'
stories_columns_to_keep = ['ref','subject','sprint',
                           'sprint_estimated_start','sprint_estimated_finish',
                           'status','total-points','tags','estimate']
epic_prefix = 'epic_'
us_prefix= 'us_'

# Replace these with the names of the columns that contain related data
epic_merge_column = epic_prefix + 'related_user_stories'
us_merge_column = us_prefix + 'ref'

ts_name = datetime.now().strftime("%Y%m%d_%H%M%S")  # crea un timestamp formattato

# filename of the google service account key json file (in the same folder of the current py script)
creds_json = '../roadmap-taiga-43902e6ad4ce.json'

# google id for the spreadsheet to use as main template for the report
spreadsheet_template_id = '15dZ4Pl_ebi1gNMAbsNW26wDvtHN4B39ZDSM'
# name to assign to the newly created report
new_spreadsheet_name = f"{taiga_project_slug} report {ts_name}"


def get_input_df(epics_url, stories_url):
    """
    get the content from taiga endpoints for epics and stories
    return two dataframe for epics and stories
    """

    # Read the CSV files from the URLs into dataframes
    print(f'getting epics from: {epics_url}')
    response1 = requests.get(epics_url)
    print(f'getting stories from: {stories_url}')
    response2 = requests.get(stories_url)
    
    df_epics = pd.read_csv(io.StringIO(response1.text))
    df_stories = pd.read_csv(io.StringIO(response2.text))
    
    return df_epics, df_stories


def cleanup_df(orig_df, prefix, cols_to_keep):
    """
    get original dataframe orig_df
    returns dataframe output_df, 
    with prefix in cols and 
    only cols in cols_to_keep
    """

    print(f"""removing unneeded cols from dataframe, leaving the following:
    {', '.join([el for el in cols_to_keep])}""")
    # Keep only the specified columns in the dataframes
    ouput_df = orig_df.loc[:, cols_to_keep]
    
    # Add prefix to all column names in the dataframes
    ouput_df = ouput_df.add_prefix(prefix)

    return ouput_df


def add_year_month_to_us(orig_df, input_col, year_col_name, month_col_name):
    """
    get original dataframe orig_df
    retuns dataframe output_df
    with added year_col_name and month_col_name cols
    calculated from specified input_col
    """

    print(f'adding year and month cols for each story')
    # remove NaN from input_col in stories
    orig_df[input_col] = orig_df[input_col].fillna('')

    # create us_year and us_months cols
    orig_df[year_col_name]= pd.DatetimeIndex(orig_df[input_col]).year.astype(str)
    orig_df[year_col_name] = orig_df[year_col_name].str.removesuffix('.0')
    orig_df[month_col_name]= pd.DatetimeIndex(orig_df[input_col]).month.astype(str)
    orig_df[month_col_name] = orig_df[month_col_name].str.removesuffix('.0')

    return orig_df


def fix_special_cols(cleaned_epics_df, cleaned_us_df, merge_column_epic, related_us_prefix):
    """
    get cleaned epics and stories df
    return fixed epics and stories df, ready to be merged
    """

    print(f'applying special fixes to cleaned_epics_df and cleaned_us_df')

    # replace with 0 the nan in the total points column
    cleaned_us_df['us_total-points'] = cleaned_us_df['us_total-points'].fillna(0)

    # Filter out rows in the epics dataframe where the merge_column_epic is NaN
    # (epics with no stories are not displyed in the report)
    cleaned_epics_df = cleaned_epics_df[cleaned_epics_df[merge_column_epic].notna()]

    # remove .0 in the year and month columns
    cleaned_epics_df.loc[:,'epic_year'] = cleaned_epics_df['epic_year'].astype(str).str.removesuffix('.0')

    # Define a function to remove the prefix from a list of IDs
    remove_prefix = lambda x: [id.replace(related_us_prefix, '') for id in x.split(',')]

    # Remove the prefix from the merge_column_epic column in the epics dataframe
    cleaned_epics_df.loc[:,merge_column_epic] = cleaned_epics_df[merge_column_epic].apply(remove_prefix)

    return cleaned_epics_df, cleaned_us_df


def get_merged_df(parent_df, children_df, merge_column_parent, merge_column_children):
    """
    get parent_df and children_df
    return a merged_df with one line per each child, 
    with all the info for related parent
    """

    print(f'computing dataframe with merged parent and children info')

    # Explode the merge_column_parent in the parent_df dataframe
    parent_df = parent_df.explode(merge_column_parent)

    # Convert the merge columns to the same data type
    parent_df[merge_column_parent] = parent_df[merge_column_parent].astype(str)
    children_df[merge_column_children] = children_df[merge_column_children].astype(str)

    # Merge the dataframes using the specified columns
    output_df = pd.merge(parent_df, 
                         children_df, 
                         left_on=merge_column_parent, 
                         right_on=merge_column_children)

    return output_df


def fix_merged_df(orig_df):
    """
    get orig_df
    return a fixed output_df with mainly string cols,
    us_total-points propely rendered as float,
    and all nan removed from string columns
    """ 

    print(f'applying special fixes to merged dataframe')

    # tranform the df to string to make sure the insert in gsheet works
    orig_df = orig_df.astype(str)
    # we want only total point column to be float
    orig_df['us_total-points'] = orig_df['us_total-points'].astype(float)

    # remove all NaN from string columns
    orig_df = orig_df.apply(lambda col: col.replace('nan', '') if col.dtype == 'object' else col)

    # DEBUG: write merged df
    # merged_df.to_csv("results/pivot_table" + ts_name + ".csv")

    return orig_df


def create_sheet_from_template(creds_json, spreadsheet_template_id, new_spreadsheet_name):
    """
    access google drive with credential in creds_json
    clone the spreadsheet_template assigning new_spreadsheet_name
    return the new_spreadsheet
    """

    print(f'accessing google services with creds in {creds_json}')

    # Set up Google credentials
    scope = ['https://spreadsheets.google.com/feeds', 
            'https://www.googleapis.com/auth/drive',
            'https://www.googleapis.com/auth/spreadsheets']
    credentials = ServiceAccountCredentials.from_json_keyfile_name(creds_json, scope)
    gc = gspread.authorize(credentials)

    print(f'creating new report in {new_spreadsheet_name}')
    # create a copy of the report template with configured name
    new_spreadsheet = gc.copy(spreadsheet_template_id, title=new_spreadsheet_name)

    return new_spreadsheet


def update_report_with_df_data(spreadsheet, df):
    """
    update the google spreadsheet based on the report template 
    with data in df exported from taiga and prepared appropriately
    """

    print(f'updating report in with data exported from taiga')

    # Select the worksheet
    worksheet = spreadsheet.worksheet('Dati')

    # Save the DataFrame to the Google Spreadsheet
    result = worksheet.update([df.columns.values.tolist()] + df.values.tolist())

    print(f'Report update completed! \n {result}')


def main():
    """
    launch main flow of operations:
    - get data from taiga
    - clean and prepare data for final report
    - clone the google report template
    - update the cloned report with prepared data from taiga
    """

    df_epics, df_stories = get_input_df(epics_url, stories_url)

    df_epics = cleanup_df(df_epics, epic_prefix, epics_columns_to_keep)
    df_stories = cleanup_df(df_stories, us_prefix, stories_columns_to_keep)

    df_stories = add_year_month_to_us(df_stories, 
                                    'us_sprint_estimated_finish', 
                                    us_prefix + 'year',
                                    us_prefix + 'month')

    df_epics, df_stories = fix_special_cols(df_epics, 
                                            df_stories, 
                                            epic_merge_column, 
                                            related_us_prefix)

    merged_df = get_merged_df(df_epics, df_stories, epic_merge_column, us_merge_column)

    merged_df = fix_merged_df(merged_df)

    report_spreadsheet = create_sheet_from_template(creds_json, spreadsheet_template_id, new_spreadsheet_name)

    update_report_with_df_data(report_spreadsheet, merged_df)



main()