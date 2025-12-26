#!/usr/bin/env python
# coding: utf-8

# In[61]:


import pandas as pd


# In[62]:


df = pd.read_csv(r"C:\Users\HP\Downloads\customer_shopping_behavior.csv")


# In[63]:


df.head()


# In[64]:


df.info()


# In[65]:


df.describe(include='all')


# In[66]:


df.isnull().sum()


# In[67]:


df['Review Rating'] = df.groupby('Category')['Review Rating'].transform(lambda x: x.fillna(x.median()))


# In[68]:


df.isnull().sum()


# In[69]:


df.columns = df.columns.str.lower()
df.columns = df.columns.str.replace(' ','_')
df = df.rename(columns={'customerid':'customer_id'})
df = df.rename(columns={'itempurchased':'item_purchased'})
df = df.rename(columns={'purchase_amount_(usd)':'purchase_amount'})
df = df.rename(columns={'reviewrating':'review_rating'})
df = df.rename(columns={'subscriptionstatus':'subscription_status'})
df = df.rename(columns={'shippingtype':'shipping_type'})
df = df.rename(columns={'discountapplied':'discount_applied'})
df = df.rename(columns={'promocodeused':'promo_code_used'})
df = df.rename(columns={'previouspurchases':'previous_purchases'})
df = df.rename(columns={'paymentmethod':'payment_method'})
df = df.rename(columns={'frequencyofpurchases':'frequency_of_purchases'})


# In[70]:


df.columns


# In[71]:


# create a column age_group
labels = ['Young Adult','Adult', 'Middle-aged', 'Senior']
df['age_group'] = pd.qcut(df['age'], q=4, labels = labels)


# In[72]:


df[['age','age_group']].head(10)


# In[73]:


# create column purchased_frequency_days

frequency_mapping = {
    'Fortnightly':14,
    'Weekly':7,
    'Monthly':30,
    'Bi-Weekly':14,
    'Annually':365,
    'Every 3 Months':90
}
df['purchase_frequency_days'] = df['frequency_of_purchases'].map(frequency_mapping)


# In[74]:


df[['purchase_frequency_days','frequency_of_purchases']].head(10)


# In[75]:


df[['discount_applied','promo_code_used']].head(10)


# In[76]:


(df['discount_applied']==df['promo_code_used']).all()


# In[77]:


df = df.drop('promo_code_used', axis=1)


# In[78]:


df.columns


# In[79]:


pip install psycopg2-binary sqlalchemy


# In[6]:


import pandas as pd

df = pd.read_csv(r"C:\Users\HP\Downloads\customer_shopping_behavior.csv")
df.head()


# In[7]:


from sqlalchemy import create_engine
from urllib.parse import quote_plus
import psycopg2

print(psycopg2.__version__)

username = "postgres"
password = quote_plus("Devakishinde@2001")
host = "localhost"
port = "5432"
database = "customer_behavior"

engine = create_engine(
    f"postgresql://{username}:{password}@{host}:{port}/{database}"
)

df.to_sql("customer", engine, if_exists="replace", index=False)

print("Data successfully loaded into PostgreSQL")


# In[ ]:




