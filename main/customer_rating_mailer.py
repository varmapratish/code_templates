"""
Project Title: Customer Rating
Project Description: Customer Rating provides a rating to every single customer/buyer based on their performance on Shiprocket
                     Platform. It helps to quickly distinguish good and not-good customers. And helps in decision making
                     process for approving and handing out benefits up to certain limits.
                     rating are calculated for customers who have done 1+ shipments in past 30 days.
First Implementation Date: april 2022
Mentor: Adit Mishra
Developers: Pratish Varma
"""


# Importing essential libraries
import os
import shutil
from datetime import date
import pandas as pd
import numpy as np
import pandas_redshift as pr

from ML.projects.utils.error_handler import auto_error_handler
from ML.projects.utils.procedure_caller import procedure_run
from ML.projects.utils.db_connection_master import *
from ML.config import config
from ML.projects.aws.s3 import S3
from ML.projects.customer_rating_project.customer_rating_preprocess import customer_rating_calculator

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.append(os.path.abspath('.'))


# Path to save project files on serverO
today = str((date.today()).strftime('%Y%m%d'))
newpath = DUMP_PATH + "customer_rating/customer_rating_metadata/" + today
script_name = "Customer Rating"


class customerrating(object):
    """
    This class fetches, preprocess and calculates the Customer Rating.
    """

    def __init__(self):
        try:
            # Redshift Connection for Local Shiprocket in DWH
            self.con, self.redshift_eng, self.redshift_cursor = db_connections.redshift_con(self, 'dwh')
            print('Connection: Connection created using db_connections.redshift_con')

        except Exception as e:
            auto_error_handler.mail(self, e, script_name)

    def calculate_customer_rating(self):
        """Fetches data using procedure, calculates rating and updates tables in Redshift"""

        try:
            # Create directory to store project output files on server
            if not os.path.exists(newpath):
                os.makedirs(newpath)
            else:
                shutil.rmtree(newpath)
                os.makedirs(newpath)

            # Run Procedure to fetch raw data
            procedure_run.redshift_procedure_call(self, 'pratish_customer_rating_daily_metadata_proc')
            print("Step 1: Procedure Complete")
            sql_query = """ select * from pratish_reports_customer_rating_daily_metadata; """
            self.df_main = pd.read_sql(sql_query, self.con)
            print('Step 2: Daily Metadata Table - Read into DataFrame')

            # Preprocess & Calculate ratings
            self.df = customer_rating_calculator(data=self.df_main)
            print("Step 3: Rating Calculated")

            # Truncate existing table
            query_1 = ''' truncate table pratish_reports_customer_rating_daily_metadata; '''
            self.redshift_cursor.execute(query_1)
            print('Step 4: Daily Metadata Table - Truncated')

            # Make connection using pandas_redshift
            db_con = pr.connect_to_redshift(dbname='dwh',
                                            host=config.SR_Redshift_Host,
                                            port=config.SR_Redshift_Port,
                                            user=config.SR_Redshift_User,
                                            password=config.PGPASSWORD)
            pr.connect_to_s3(aws_access_key_id=config.aws_access_key_id,
                             aws_secret_access_key=config.aws_secret_access_key,
                             bucket=config.bucket,
                             subdirectory='couriers_etl'
                             # As of release 1.1.1 you are able to specify an aws_session_token (if necessary):
                             # aws_session_token = <aws_session_token>
                             )
            print('Connection: Connection created using pandas_redshift')

            try : 
                query_hist_previous_data = """ 
                select customer_phone , customer_rating ,bins_rating
                from pratish_reports_customer_rating_history; 
                """
                print(query_hist_previous_data)
                self.previous_df = pd.read_sql(query_hist_previous_data, self.con)
            except Exception as e:
                print(e)
                print("History Table doesnt exist!!")


            # Insert data from Pandas DataFrame to Redshift: combined raw data and scores
            # If the table currently exists IT WILL BE DROPPED and then the pandas DataFrame will be put in it's place.
            # If you set append = True the table will be appended to (if it exists).
            pr.pandas_to_redshift(data_frame=self.df, redshift_table_name='pratish_reports_customer_rating_daily_metadata',
                                  delimiter='|', append=False)
            print('Step 5: Daily Metadata Table - Data w/ ratings Inserted')

            pr.close_up_shop()
            print('Connection: Connection closed for pandas_redshift')

            # Create/ Update History Table
            # Update entries for all sellers whose latest ratings are available from above calculation.
            query_3 = '''
                create table if not exists pratish_reports_customer_rating_history
                (customer_phone varchar(500),
                last_assigned_date date,
                last_30_days_shipment bigint,
                last_30_days_value real,  
                last_30_days_is_return_shipment bigint,   
                last_30_days_prepaid_orders bigint,
                last_30_days_recency real,
                shipments_ltv real,
                revenue_ltv real,
                age bigint,   
                loyalty bigint,
                last_5months_shipment real,
                last_5months_value real,
                last_5months_is_return_shipment real,
                last_5months_prepaid_orders real,
                last_5months_recency real,
                last_5months_shipments_ltv real,
                last_5months_revenue_ltv real,
                last_5months_age bigint,   
                last_5months_loyalty bigint,
                first_shipment_date date,
                last_5months_last_assigned_date date,
                last_30_days_pop real,
                last_30_days_rto_percentage real,
                last_5months_pop real, 
                last_5months_rto_percentage real,
                rps real,
                rating_30days real,
                rating_5months real,
                customer_rating real,
                bins_rating varchar(256),
                rating_category varchar(256),
                limit real , 
                updated_at datetime
                );
                commit;
               
                
                
                
                DELETE FROM pratish_reports_customer_rating_history 
                where customer_phone in (select customer_phone from pratish_reports_customer_rating_daily_metadata);
                commit;

                INSERT INTO pratish_reports_customer_rating_history (customer_phone ,last_assigned_date ,last_30_days_shipment ,last_30_days_value ,last_30_days_is_return_shipment ,last_30_days_prepaid_orders ,last_30_days_recency ,shipments_ltv ,revenue_ltv ,age ,loyalty ,last_5months_shipment ,last_5months_value ,last_5months_is_return_shipment ,last_5months_prepaid_orders ,last_5months_recency  ,last_5months_shipments_ltv ,last_5months_revenue_ltv ,last_5months_age ,last_5months_loyalty  ,first_shipment_date ,last_5months_last_assigned_date  ,last_30_days_pop  ,last_30_days_rto_percentage ,last_5months_pop ,last_5months_rto_percentage ,rating_30days,rating_5months ,rps ,customer_rating ,bins_rating ,rating_category ,limit ,updated_at)
                                                              SELECT customer_phone ,last_assigned_date ,last_30_days_shipment ,last_30_days_value ,last_30_days_is_return_shipment ,last_30_days_prepaid_orders ,last_30_days_recency ,shipments_ltv ,revenue_ltv ,age ,loyalty ,last_5months_shipment ,last_5months_value ,last_5months_is_return_shipment ,last_5months_prepaid_orders ,last_5months_recency  ,last_5months_shipments_ltv ,last_5months_revenue_ltv ,last_5months_age ,last_5months_loyalty  ,first_shipment_date ,last_5months_last_assigned_date  ,last_30_days_pop  ,last_30_days_rto_percentage ,last_5months_pop ,last_5months_rto_percentage ,rating_30days,rating_5months ,rps ,customer_rating ,bins_rating ,rating_category ,limit ,updated_at
                FROM pratish_reports_customer_rating_daily_metadata;
                commit;
                '''

            self.redshift_cursor.execute(query_3)
            print('Step 6: History Metadata Table - Rows Updated')

            ######################################################
            ######################################################

            # Save last 30days ratings in a table
            query_3a = '''
                create table if not exists pratish_reports_customer_rating_history_15d
                (customer_phone varchar(500),
                last_assigned_date date,
                last_30_days_shipment bigint,
                last_30_days_value real,  
                last_30_days_is_return_shipment bigint,   
                last_30_days_prepaid_orders bigint,
                last_30_days_recency real,
                shipments_ltv real,
                revenue_ltv real,
                age bigint,   
                loyalty bigint,
                last_5months_shipment real,
                last_5months_value real,
                last_5months_is_return_shipment real,
                last_5months_prepaid_orders real,
                last_5months_recency real,
                last_5months_shipments_ltv real,
                last_5months_revenue_ltv real,
                last_5months_age bigint,   
                last_5months_loyalty bigint,
                first_shipment_date date,
                last_5months_last_assigned_date date,
                last_30_days_pop real,
                last_30_days_rto_percentage real,
                last_5months_pop real, 
                last_5months_rto_percentage real,
                rps real,
                rating_30days real,
                rating_5months real,
                customer_rating real,
                bins_rating varchar(256),
                rating_category varchar(256),
                limit real ,
                updated_at datetime
                );
                commit;
                
                INSERT INTO pratish_reports_customer_rating_history_15d (customer_phone ,last_assigned_date ,last_30_days_shipment ,last_30_days_value ,last_30_days_is_return_shipment ,last_30_days_prepaid_orders ,last_30_days_recency ,shipments_ltv ,revenue_ltv ,age ,loyalty ,last_5months_shipment ,last_5months_value ,last_5months_is_return_shipment ,last_5months_prepaid_orders ,last_5months_recency  ,last_5months_shipments_ltv ,last_5months_revenue_ltv ,last_5months_age ,last_5months_loyalty  ,first_shipment_date ,last_5months_last_assigned_date  ,last_30_days_pop  ,last_30_days_rto_percentage ,last_5months_pop ,last_5months_rto_percentage ,rating_30days,rating_5months ,rps ,customer_rating ,bins_rating ,rating_category ,limit ,updated_at)
                                                                  SELECT customer_phone ,last_assigned_date ,last_30_days_shipment ,last_30_days_value ,last_30_days_is_return_shipment ,last_30_days_prepaid_orders ,last_30_days_recency ,shipments_ltv ,revenue_ltv ,age ,loyalty ,last_5months_shipment ,last_5months_value ,last_5months_is_return_shipment ,last_5months_prepaid_orders ,last_5months_recency  ,last_5months_shipments_ltv ,last_5months_revenue_ltv ,last_5months_age ,last_5months_loyalty  ,first_shipment_date ,last_5months_last_assigned_date  ,last_30_days_pop  ,last_30_days_rto_percentage ,last_5months_pop ,last_5months_rto_percentage ,rating_30days,rating_5months ,rps ,customer_rating ,bins_rating ,rating_category ,limit ,updated_at
                FROM pratish_reports_customer_rating_daily_metadata;
                commit;
    
                DELETE FROM pratish_reports_customer_rating_history_15d 
                where to_char(updated_at, 'YYYYMMDD') < to_char(dateadd(day, -15, getdate()), 'YYYYMMDD');
                commit;
                '''

            self.redshift_cursor.execute(query_3a)
            print('Step 6a: 30 days History Metadata Table - Rows Added')



            ######################################################
            ######################################################

            # Read History table from db.
            query_hist_data = """ 
            select customer_phone ,last_assigned_date ,last_30_days_shipment ,last_30_days_value ,last_30_days_is_return_shipment ,last_30_days_prepaid_orders ,last_30_days_recency ,shipments_ltv ,revenue_ltv ,age ,loyalty ,last_5months_shipment ,last_5months_value ,last_5months_is_return_shipment ,last_5months_prepaid_orders ,last_5months_recency  ,last_5months_shipments_ltv ,last_5months_revenue_ltv ,last_5months_age ,last_5months_loyalty  ,first_shipment_date ,last_5months_last_assigned_date  ,last_30_days_pop  ,last_30_days_rto_percentage ,last_5months_pop ,last_5months_rto_percentage ,rps ,rating_30days,rating_5months ,customer_rating ,bins_rating ,rating_category ,limit , updated_at
            from pratish_reports_customer_rating_history; 
            """
            print(query_hist_data)
            self.hist_df = pd.read_sql(query_hist_data, self.con)

            self.con.close()
            print('Connection: Connection closed for db_connections.redshift_con')

        except Exception as e:
            auto_error_handler.mail(self, e, script_name)


    def mailer(self):
        """Prepares the text body and sends the mailer."""

        try:
            todays_date = datetime.today()

            
            

            # Mail Body
            body_string = "<html><body><br>"
            body_string += f"<h1 style='color: #134f5c; text-align:center;'>Customer Rating Analytics {todays_date.strftime('%Y-%m-%d')}</h1>"

            # body_string += "<p>Analytics/Scores are calculated majorly based on Past 3 Months data.</p>"
            body_string += f"<p><strong>Total Customers who did 1+ shipments in past 30 days: </strong>{self.df_main.shape[0]}<br>"
            # body_string += f"<strong>Above Cutomers with 1+ Shipments: </strong>{self.df.shape[0]}<br>"
            # body_string += f"<strong>Download the report: </strong>{url}</p>"
            body_string += "<br>"

    




            body_string += "<h3 style='color: #45818e;'>1. Category Wise</h3>"
            body_string += "<p><strong>Good:</strong> 4-5 rating<br>"
            body_string += "<strong>Fine:</strong> 3-4 rating<br>"
            body_string += "<strong>Bad:</strong> 0-3 rating</p>"





            body_string += "<p><strong>Historical ratings (Since april 8 2022)</strong><br>"
            body_string += "Contains information for all the customers including those whose rating have ever been calculated in the past</p>"
            body_string += f"<p><strong>Total Customers whos rating ever been calculated: </strong>{self.hist_df.shape[0]}<br>"

            bins = [-1.0,0.0,1.0,2.0,3.0,4.0,5.0]
            t = pd.cut(self.hist_df['customer_rating'],bins).value_counts().sort_index().reset_index()
            t['%'] = round((t['customer_rating']/t['customer_rating'].sum())*100,2)
            t.rename(columns = {'customer_rating':'count','index':'customer_rating_bucket'},inplace=True)


            body_string += f"{(t).to_html()}"

            body_string += "<br>"

            try : 
                self.previous_df.rename(columns = {'customer_rating': 'customer_rating_previous','bins_rating':'bins_rating_previous'}, inplace=True)
                self.previous_df = self.previous_df.merge(self.hist_df[['customer_phone','customer_rating','bins_rating']],on='customer_phone',how='left')

                body_string += "<h3 style='color: #45818e;'>2. Crosstab of Rating changes from previous calculated ratings</h3>"

                body_string += "{}".format(pd.crosstab(self.previous_df['bins_rating'], self.previous_df['bins_rating_previous']).to_html())
                body_string += "<br>"
                body_string += "<h3 style='color: #45818e;'>3. Above crosstab in percentage</h3>"
                

                body_string += "{}".format(round(pd.crosstab(self.previous_df['bins_rating'], self.previous_df['bins_rating_previous'],normalize=True)*100,2).to_html())

                l = len(self.hist_df[self.hist_df['customer_rating'].isna()])

                body_string += "<br><strong>Count of values of final rating is NAN :  </strong>{}<br>".format(l)

                self.previous_df['differance_score'] = self.previous_df['customer_rating_previous']-self.previous_df['customer_rating']

                l2 = len(self.previous_df[(self.previous_df['differance_score']>=3)|(self.previous_df['differance_score']<=-3)])

                body_string += "<strong>Count of rating difference greater than 3 :  </strong>{}".format(l2)

                body_string += "<br>"


            except Exception as e:
                print(e)

                print("Table Doesnt Exist!!")
                body_string += "<p><strong>Previous Table Doesn't exist !!</strong><br>"




            
            body_string += "</body></html>"

            # Mail Details
            mail_data = dict()
            mail_data['from'] = "noreply@kraftly.com"
            mail_data['to'] = ["ankit.yadav@shiprocket.com", "pratish.varma@shiprocket.com"]
            mail_data['subject'] = f"Customer Rating Report {todays_date.strftime('%Y-%m-%d %H:%M')}"

            mail_data['body'] = body_string
            print(newpath)
            se = SendGrid()
            send = se.send_mail(mail_data)
            if send:
                print('Sent')
            else:
                print('Not Sent')

        except Exception as e:
            auto_error_handler.mail(self, e, script_name)





