import pandas as pd
import numpy as np

# =============================================================================
# 1. Carregamento dos Dados
# =============================================================================
df_suicide = pd.read_csv('suicide_rate_by_country_2024.csv')
df_social  = pd.read_csv('Social_Progress_Index_2022.csv')
df_country = pd.read_csv('country_data.csv')
df_WHR     = pd.read_csv('WHR2024.csv')

# =============================================================================
# 2. Standardização dos Nomes dos Países
# =============================================================================
def standardize_country_names(df, country_col):
    df_std = df.copy()
    country_mapping = {
        'United States': 'United States of America',
        'United States Virgin Islands': 'United States of America',
        'USA': 'United States of America',
        'US': 'United States of America',
        'United Kingdom': 'United Kingdom',
        'UK': 'United Kingdom',
        'Great Britain': 'United Kingdom',
        'Brunei Darussalam': 'Brunei',
        'Bolivia, Plurinational State Of': 'Bolivia',
        'Cabo Verde': 'Cape Verde',
        'Congo (Brazzaville)': 'Republic of the Congo',
        'Congo (Kinshasa)': 'Democratic Republic of the Congo',
        'Dr Congo': 'Democratic Republic of the Congo',
        'Congo': 'Democratic Republic of the Congo',
        'Congo, Democratic Republic Of': 'Democratic Republic of the Congo',
        'Congo, Republic Of': 'Republic of the Congo',
        'Congo, The Democratic Republic Of The': 'Democratic Republic of the Congo',
        'Korea, Republic of': 'South Korea',
        'Korea, Democratic People\'s Republic Of': 'North Korea',
        'Russian Federation': 'Russia',
        'Ivory Coast': "Côte d'Ivoire",
        "Cote D'Ivoire": "Côte d'Ivoire",
        'Czech Republic': 'Czechia',
        'Moldova, Republic Of': 'Moldova',
        'Micronesia, Federated States Of': 'Micronesia',
        'Macedonia, The Former Yugoslav Republic Of': 'Macedonia',
        'North Macedonia': 'Macedonia',
        'Lao People\'S Democratic Republic': 'Laos',
        'Republic Of North Macedonia': 'Macedonia',
        'Viet Nam': 'Vietnam',
        'Turkey': 'Türkiye',
        'Turkiye': 'Türkiye',
        'Iran, Islamic Republic Of': 'Iran',
        'Syria': 'Syrian Arab Republic',
        'Gambia, The': 'The Gambia',
        'Swaziland': 'Eswatini',
        'Palestine': 'State of Palestine',
        'Taiwan Province of China': 'Taiwan',
        'Tanzania, United Republic Of': 'Tanzania',
        'Hong Kong S.A.R. Of China': 'Hong Kong',
        'Venezuela, Bolivarian Republic Of': 'Venezuela'
    }
    
    if country_col in df_std.columns:
        df_std[country_col] = df_std[country_col].replace(country_mapping)
        df_std[country_col] = df_std[country_col].str.strip()
    
    return df_std

# =============================================================================
# 3. Preparação dos DataFrames
# =============================================================================
def prepare_dataframe(df, country_col, target_country_col='country'):
    df_prep = df.copy()
    df_prep.rename(columns={country_col: target_country_col}, inplace=True)
    df_prep = standardize_country_names(df_prep, target_country_col)
    return df_prep

# =============================================================================
# 4. Merge DataFrames e tratar valores duplicados
# =============================================================================
def merge_datasets(*dfs):
    final_df = dfs[0]

    for df in dfs[1:]:
        final_df = pd.merge(final_df, df, on='country', how='outer')

    def smart_combine(group):
        combined = {}

        for col in group.columns:
            if col == 'country':
                combined[col] = group[col].iloc[0]
                continue

            non_null_values = group[col].dropna().values

            if len(non_null_values) == 0:
                combined[col] = np.nan
            elif len(non_null_values) == 1:
                combined[col] = non_null_values[0]
            else:
                numeric_values = pd.to_numeric(non_null_values, errors='coerce')
                numeric_values = pd.Series(numeric_values).dropna()

                if len(numeric_values) > 0:
                    combined[col] = numeric_values.mean()
                else:
                    combined[col] = np.nan

        return pd.Series(combined)

    final_df = final_df.groupby('country', as_index=False).apply(smart_combine)
    return final_df

# =============================================================================
# 5. Seleção e preparação dos DataFrames
# =============================================================================
df_WHR_selected = df_WHR[['Country name', 'Ladder score', 'Explained by: Log GDP per capita', 
                          'Explained by: Social support', 'Explained by: Healthy life expectancy', 
                          'Explained by: Freedom to make life choices', 'Explained by: Generosity', 
                          'Explained by: Perceptions of corruption']]

df_country_selected = df_country[['name', 'gdp', 'life_expectancy_male', 'unemployment', 'imports', 
                                  'exports', 'homicide_rate', 'urban_population_growth', 
                                  'secondary_school_enrollment_female', 'forested_area', 
                                  'post_secondary_enrollment_female', 'post_secondary_enrollment_male', 
                                  'primary_school_enrollment_female', 'infant_mortality', 'gdp_growth', 
                                  'population', 'secondary_school_enrollment_male', 'pop_growth',
                                  'pop_density', 'internet_users', 'fertility', 'refugees', 
                                  'primary_school_enrollment_male', 'co2_emissions', 'tourists']]

df_social_selected = df_social[['Country', 'Social Progress Score', 'Basic Human Needs', 
                                'Foundations of Wellbeing', 'Opportunity', 'Nutrition and Basic Medical Care', 
                                'Water and Sanitation', 'Shelter', 'Personal Safety', 
                                'Access to Basic Knowledge', 'Access to Information and Communications', 
                                'Health and Wellness', 'Environmental Quality', 'Personal Rights', 
                                'Personal Freedom and Choice', 'Inclusiveness', 'Access to Advanced Education']]

df_suicide_selected = df_suicide[['country', 'SuicideRate_BothSexes_RatePer100k_2021', 
                                  'SuicideRate_Male_RatePer100k_2021', 'SuicideRate_Female_RatePer100k_2021']]

df_WHR_prep = prepare_dataframe(df_WHR_selected, 'Country name')
df_country_prep = prepare_dataframe(df_country_selected, 'name')
df_social_prep = prepare_dataframe(df_social_selected, 'Country')
df_suicide_prep = prepare_dataframe(df_suicide_selected, 'country')

# =============================================================================
# 6. Merge dos DataFrames
# =============================================================================
final_df = merge_datasets(df_WHR_prep, df_country_prep, df_social_prep, df_suicide_prep)

# =============================================================================
# 7. Limpeza dos países com >50% valores omissos e preencher na com média
# =============================================================================
def clean_dataset(df):

    threshold = len(df.columns) // 2
    df_cleaned = df.dropna(thresh=threshold, axis=0)

    numeric_cols = df_cleaned.select_dtypes(include=["number"]).columns
    df_cleaned[numeric_cols] = df_cleaned[numeric_cols].apply(lambda x: x.fillna(x.mean()))

    return df_cleaned

cleaned_df = clean_dataset(final_df)

# =============================================================================
# 8. Guardar em xlsx
# =============================================================================
output_filename = 'merged_dataset.xlsx'
cleaned_df.to_excel(output_filename, index=False)
print(f"File saved as {output_filename}")