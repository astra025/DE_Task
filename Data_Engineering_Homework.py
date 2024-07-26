# Databricks notebook source
# MAGIC %md
# MAGIC ### Define Data

# COMMAND ----------

# Test dataset

import pandas as pd

df_1 = pd.DataFrame(
    data=[
        ['A','a', 'x', 1],
        ['A','b', 'x', 1],
        ['A','c', 'x', 1],
        ['B','a', 'x', 1],
        ['B','b', 'x', 1],
        ['B','c', 'x', 1],
        ['A','a', 'y', 1],
    ],
    columns=['col_1', 'col_2', 'col_3', 'col_4']
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Function definition for duplicates

# COMMAND ----------

def duplicateChecker(df, columns):
    try:
        # Group by the specified columns and count the occurrences
        grouped = df.groupby(columns).size().reset_index(name='Number of duplicates')
        
        # Filter out groups with only one occurrence (no duplicates)
        duplicates = grouped[grouped['Number of duplicates'] > 1]

        # Count the number of duplicate cases
        count = duplicates.shape[0]
        
        if count>0:
        # Return the result as a dictionary
            result = {
                'count': count,
                'samples': duplicates
            }
            return result
        else:
            print("There are no duplicates for the selected criteria")
            return ""
    
    except Exception as error:
        print(error)
        return "" 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Call the function and test with different set of columns

# COMMAND ----------

# Testing the function

# Define the columns 
cols1 = ['col_1']
cols2 = ['col_1', 'col_2']
cols3 = ['col_1', 'col_2', 'col_3']
cols4 = ['col_1', 'col_2', 'col_3','col_4']

# Test Case 1
print("Duplicates for cols1 :")
print(duplicateChecker(df_1, cols1))

# Test Case 2
print("\nDuplicates for cols2 :")
print(duplicateChecker(df_1, cols2))

# Test Case 3
print("\nDuplicates for cols3 :")
print(duplicateChecker(df_1, cols3))

# Test Case 4
print("\nDuplicates for cols4 :")
duplicateChecker(df_1, cols4)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Error handling testing

# COMMAND ----------


#Pass extra unknown column
cols5 =['col_1', 'col_2', 'col_3','col_4', 'col_5']
duplicateChecker(df_1, cols5)
