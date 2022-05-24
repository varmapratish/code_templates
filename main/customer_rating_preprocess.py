import pandas as pd
import numpy as np
import os
import datetime
from datetime import datetime
import datetime as dt
from datetime import date




def customer_rating_calculator(data):

    data=data[data['customer_phone'].notna()]


    def str_to_dt(data):
        try:
            return datetime.strptime(data, "%Y-%m-%d")
        except:
            return data

    data['last_30_days_pop'] = data['last_30_days_prepaid_orders']/data['last_30_days_shipment']
    data['last_5months_pop'] = data['last_5months_prepaid_orders']/data['last_5months_shipment']

    data['first_shipment_date'] = pd.to_datetime(data['first_shipment_date'])
    data['last_assigned_date'] = pd.to_datetime(data['last_assigned_date'])

    data['last_5months_last_assigned_date']= pd.to_datetime(data['last_5months_last_assigned_date'])

    # date_str = '2022-04-29'
    # date = datetime.strptime(date_str, "%Y-%m-%d")
    data['age'] = (datetime.now() - data['first_shipment_date']).dt.days
    data['loyalty'] = (data['last_assigned_date'] - data['first_shipment_date']).dt.days


    data['last_5months_loyalty'] = (data['last_5months_last_assigned_date'] - data['first_shipment_date']).dt.days
    data['last_5months_age'] = data['age']-30

    data['last_5months_rto_percentage'] = data['last_5months_is_return_shipment']/data['last_5months_shipment']
    data['last_30_days_rto_percentage'] = data['last_30_days_is_return_shipment']/data['last_30_days_shipment']

    def GT5_30days(greater_than_5):
        greater_than_5['shipment_proportion_1'] = greater_than_5['last_30_days_shipment'].apply(lambda x : 1 if x >= 20 else(
                                                                                                0.9 if 15 < x < 20 else(
                                                                                                0.8 if 10 < x <= 15 else(
                                                                                                0.7 if 5 < x <= 10 else(0)))))
        
        greater_than_5.loc[greater_than_5['last_30_days_recency'] <=10,'shipment_proportion_2'] = 1
        greater_than_5.loc[(greater_than_5['last_30_days_recency'] <=20)&(greater_than_5['last_30_days_recency'] >10),'shipment_proportion_2'] = 0.8
        greater_than_5.loc[(greater_than_5['last_30_days_recency'] <=30)&(greater_than_5['last_30_days_recency'] >20),'shipment_proportion_2'] = 0.6
        greater_than_5.loc[greater_than_5['last_30_days_recency'] > 30,'shipment_proportion_2'] = 0.5
        
        greater_than_5['shipment_proportion'] = greater_than_5['shipment_proportion_1'] * greater_than_5['shipment_proportion_2'] 
        
 
        greater_than_5.loc[greater_than_5['last_30_days_pop'] > 0.70,'psp_proportion_2'] = 1
        greater_than_5.loc[(greater_than_5['last_30_days_pop'] <=0.70)&(greater_than_5['last_30_days_pop'] >0.50),'psp_proportion_2'] = 0.75
        greater_than_5.loc[greater_than_5['last_30_days_pop'] <=0.50,'psp_proportion_2'] = 0.5
        
        
        greater_than_5['psp_proportion'] = greater_than_5['shipment_proportion_1'] * greater_than_5['psp_proportion_2'] 
        
        greater_than_5['loyalty_proportion_1'] = greater_than_5['loyalty'].apply(lambda x : 1 if x >= 365 else(
                                                                                    0.8 if 180 < x < 365 else(
                                                                                    0.6 if 90 < x <= 180 else(0.5))))
        
        
        greater_than_5.loc[greater_than_5['last_30_days_recency'] <=10,'loyalty_proportion_2'] = 1
        greater_than_5.loc[(greater_than_5['last_30_days_recency'] <=20)&(greater_than_5['last_30_days_recency'] >10),'loyalty_proportion_2'] = 0.8
        greater_than_5.loc[(greater_than_5['last_30_days_recency'] <=30)&(greater_than_5['last_30_days_recency'] >20),'loyalty_proportion_2'] = 0.6
        greater_than_5.loc[greater_than_5['last_30_days_recency'] > 30,'loyalty_proportion_2'] = 0.5
        
        
        greater_than_5['loyalty_proportion'] = greater_than_5['loyalty_proportion_1'] * greater_than_5['loyalty_proportion_2']
        
        
        greater_than_5['ltv_shipment_proportion'] = greater_than_5['shipments_ltv'].apply(lambda x : 1 if x >= 50 else(
                                                                                    0.8 if 20 <= x < 50 else(
                                                                                    0.7 if 10 <= x < 20 else(
                                                                                    0.6 if 0 < x < 10 else (0)))))


        greater_than_5.loc[(greater_than_5['age'] < 90)&(greater_than_5['shipments_ltv'] < 50),'ltv_shipment_proportion_2'] = 1
        greater_than_5.loc[(greater_than_5['age'] <=180)&(greater_than_5['age'] >90)&(greater_than_5['shipments_ltv'] < 50),'ltv_shipment_proportion_2'] = 0.8
        greater_than_5.loc[(greater_than_5['age'] <=365)&(greater_than_5['age'] >180)&(greater_than_5['shipments_ltv'] < 50),'ltv_shipment_proportion_2'] = 0.6
        greater_than_5.loc[(greater_than_5['age'] > 365)&(greater_than_5['shipments_ltv'] < 50),'ltv_shipment_proportion_2'] = 0.4

        greater_than_5.loc[greater_than_5['ltv_shipment_proportion_2'].isna(),'ltv_shipment_proportion_2']=1

        greater_than_5['ltv_shipment_proportion'] = greater_than_5['ltv_shipment_proportion'] * greater_than_5['ltv_shipment_proportion_2']

        weightage_shipment = 0.25
        weightage_psp = 0.25
        weightage_loyalty = 0.25
        weightage_ltv = 0.25


        proportion_cols = ['shipment_proportion','psp_proportion','loyalty_proportion','ltv_shipment_proportion']
        weightage_cols = [weightage_shipment,weightage_psp,weightage_loyalty,weightage_ltv]


        TOTAL_SCORE = 5
        greater_than_5['score'] = 0
        for p, w in zip(proportion_cols, weightage_cols):
            greater_than_5['score'] += greater_than_5[p] * w * TOTAL_SCORE
            
        
        

        greater_than_5.loc[greater_than_5['last_30_days_rto_percentage']>=0.80,'score']=greater_than_5\
                                                    [greater_than_5['last_30_days_rto_percentage']>=0.80]['score']-1
        greater_than_5.loc[(greater_than_5['last_30_days_rto_percentage']>0.4)&(greater_than_5['last_30_days_rto_percentage']<0.80),'score']=greater_than_5\
                                                                    [(greater_than_5['last_30_days_rto_percentage']>0.4)&(greater_than_5['last_30_days_rto_percentage']<0.80)]['score']-0.5
        
        return greater_than_5


    def LT5_30days(less_than_5):

        less_than_5['shipment_proportion_1'] = less_than_5['last_30_days_shipment'].apply(lambda x : 0.75 if x == 2 or x == 3 else(
                                                                                    1 if x == 4 or x==5 else(0)))

        less_than_5.loc[less_than_5['last_30_days_recency'] <=10,'shipment_proportion_2'] = 1
        less_than_5.loc[(less_than_5['last_30_days_recency'] <=20)&(less_than_5['last_30_days_recency'] >10),'shipment_proportion_2'] = 0.8
        less_than_5.loc[(less_than_5['last_30_days_recency'] <=30)&(less_than_5['last_30_days_recency'] >20),'shipment_proportion_2'] = 0.6
        less_than_5.loc[less_than_5['last_30_days_recency'] > 30,'shipment_proportion_2'] = 0.5

        less_than_5['shipment_proportion'] = less_than_5['shipment_proportion_1'] * less_than_5['shipment_proportion_2'] 



        less_than_5.loc[less_than_5['last_30_days_pop'] > 0.70,'psp_proportion_2'] = 1
        less_than_5.loc[(less_than_5['last_30_days_pop'] <=0.70)&(less_than_5['last_30_days_pop'] >0.50),'psp_proportion_2'] = 0.75
        less_than_5.loc[less_than_5['last_30_days_pop'] <=0.50,'psp_proportion_2'] = 0.5


        less_than_5['psp_proportion'] = less_than_5['shipment_proportion_1'] * less_than_5['psp_proportion_2'] 


        less_than_5['loyalty_proportion'] = less_than_5['loyalty'].apply(lambda x : 1 if x >= 365 else(
                                                                0.8 if 180 < x < 365 else(
                                                                0.6 if 90 < x <= 180 else(0.5))))

        less_than_5.loc[less_than_5['last_30_days_recency'] <=10,'loyalty_proportion_2'] = 1
        less_than_5.loc[(less_than_5['last_30_days_recency'] <=20)&(less_than_5['last_30_days_recency'] >10),'loyalty_proportion_2'] = 0.8
        less_than_5.loc[(less_than_5['last_30_days_recency'] <=30)&(less_than_5['last_30_days_recency'] >20),'loyalty_proportion_2'] = 0.6
        less_than_5.loc[less_than_5['last_30_days_recency'] > 30,'loyalty_proportion_2'] = 0.5


        less_than_5['loyalty_proportion'] = less_than_5['loyalty_proportion'] * less_than_5['loyalty_proportion_2']


        less_than_5['ltv_shipment_proportion'] = less_than_5['shipments_ltv'].apply(lambda x : 1 if x >= 50 else(
                                                                            0.8 if 20 <= x < 50 else(
                                                                            0.7 if 10 <= x < 20 else(
                                                                            0.6 if 0 < x < 10 else (0)))))


        less_than_5.loc[(less_than_5['age'] < 90)&(less_than_5['shipments_ltv'] < 50),'ltv_shipment_proportion_2'] = 1
        less_than_5.loc[(less_than_5['age'] <=180)&(less_than_5['age'] >90)&(less_than_5['shipments_ltv'] < 50),'ltv_shipment_proportion_2'] = 0.8
        less_than_5.loc[(less_than_5['age'] <=365)&(less_than_5['age'] >180)&(less_than_5['shipments_ltv'] < 50),'ltv_shipment_proportion_2'] = 0.6
        less_than_5.loc[(less_than_5['age'] > 365)&(less_than_5['shipments_ltv'] < 50),'ltv_shipment_proportion_2'] = 0.4


        less_than_5.loc[less_than_5['ltv_shipment_proportion_2'].isna(),'ltv_shipment_proportion_2']=1

        less_than_5['ltv_shipment_proportion'] = less_than_5['ltv_shipment_proportion'] * less_than_5['ltv_shipment_proportion_2']


        weightage_shipment = 0.25
        weightage_psp = 0.25
        weightage_loyalty = 0.25
        weightage_ltv = 0.25


        proportion_cols = ['shipment_proportion','psp_proportion','loyalty_proportion','ltv_shipment_proportion']
        weightage_cols = [weightage_shipment,weightage_psp,weightage_loyalty,weightage_ltv]


        TOTAL_SCORE = 5
        less_than_5['score'] = 0
        for p, w in zip(proportion_cols, weightage_cols):
            less_than_5['score'] += less_than_5[p] * w * TOTAL_SCORE





        less_than_5.loc[less_than_5['last_30_days_rto_percentage']>=0.80,'score']=less_than_5\
                                                        [less_than_5['last_30_days_rto_percentage']>=0.80]['score']-1
        less_than_5.loc[(less_than_5['last_30_days_rto_percentage']>0.55)&(less_than_5['last_30_days_rto_percentage']<0.80),'score']=less_than_5\
                                                        [(less_than_5['last_30_days_rto_percentage']>0.55)&(less_than_5['last_30_days_rto_percentage']<0.80)]['score']-0.5
        
        return less_than_5


    def GT5_5months(greater_than_5):
        greater_than_5['shipment_proportion_1'] = greater_than_5['last_5months_shipment'].apply(lambda x : 1 if x >= 20 else(
                                                                                                0.9 if 15 < x < 20 else(
                                                                                                0.8 if 10 < x <= 15 else(
                                                                                                0.7 if 5 < x <= 10 else(0)))))
        
        greater_than_5.loc[greater_than_5['last_5months_recency'] <=10,'shipment_proportion_2'] = 1
        greater_than_5.loc[(greater_than_5['last_5months_recency'] <=20)&(greater_than_5['last_5months_recency'] >10),'shipment_proportion_2'] = 0.8
        greater_than_5.loc[(greater_than_5['last_5months_recency'] <=30)&(greater_than_5['last_5months_recency'] >20),'shipment_proportion_2'] = 0.6
        greater_than_5.loc[greater_than_5['last_5months_recency'] > 30,'shipment_proportion_2'] = 0.5
        
        greater_than_5['shipment_proportion'] = greater_than_5['shipment_proportion_1'] * greater_than_5['shipment_proportion_2'] 
        
        greater_than_5.loc[greater_than_5['last_5months_pop'] > 0.70,'psp_proportion_2'] = 1
        greater_than_5.loc[(greater_than_5['last_5months_pop'] <=0.70)&(greater_than_5['last_5months_pop'] >0.50),'psp_proportion_2'] = 0.75
        greater_than_5.loc[greater_than_5['last_5months_pop'] <=0.50,'psp_proportion_2'] = 0.5
        
        
        greater_than_5['psp_proportion'] = greater_than_5['shipment_proportion_1'] * greater_than_5['psp_proportion_2'] 
        
        greater_than_5['loyalty_proportion_1'] = greater_than_5['last_5months_loyalty'].apply(lambda x : 1 if x >= 365 else(
                                                                                    0.8 if 180 < x < 365 else(
                                                                                    0.6 if 90 < x <= 180 else(0.5))))
        
        
        greater_than_5.loc[greater_than_5['last_5months_recency'] <=10,'loyalty_proportion_2'] = 1
        greater_than_5.loc[(greater_than_5['last_5months_recency'] <=20)&(greater_than_5['last_5months_recency'] >10),'loyalty_proportion_2'] = 0.8
        greater_than_5.loc[(greater_than_5['last_5months_recency'] <=30)&(greater_than_5['last_5months_recency'] >20),'loyalty_proportion_2'] = 0.6
        greater_than_5.loc[greater_than_5['last_5months_recency'] > 30,'loyalty_proportion_2'] = 0.5
        
        
        greater_than_5['loyalty_proportion'] = greater_than_5['loyalty_proportion_1'] * greater_than_5['loyalty_proportion_2']
        
        
        
        greater_than_5['ltv_shipment_proportion'] = greater_than_5['last_5months_shipments_ltv'].apply(lambda x : 1 if x >= 50 else(
                                                                                                                0.8 if 20 <= x < 50 else(
                                                                                                                0.7 if 10 <= x < 20 else(
                                                                                                                0.6 if 0 < x < 10 else (0)))))


        greater_than_5.loc[(greater_than_5['last_5months_age'] < 90)&(greater_than_5['last_5months_shipments_ltv'] < 50),'ltv_shipment_proportion_2'] = 1
        greater_than_5.loc[(greater_than_5['last_5months_age'] <=180)&(greater_than_5['last_5months_age'] >90)&(greater_than_5['last_5months_shipments_ltv'] < 50),'ltv_shipment_proportion_2'] = 0.8
        greater_than_5.loc[(greater_than_5['last_5months_age'] <=365)&(greater_than_5['last_5months_age'] >180)&(greater_than_5['last_5months_shipments_ltv'] < 50),'ltv_shipment_proportion_2'] = 0.6
        greater_than_5.loc[(greater_than_5['last_5months_age'] > 365)&(greater_than_5['last_5months_shipments_ltv'] < 50),'ltv_shipment_proportion_2'] = 0.4

        greater_than_5.loc[greater_than_5['ltv_shipment_proportion_2'].isna(),'ltv_shipment_proportion_2']=1

        greater_than_5['ltv_shipment_proportion'] = greater_than_5['ltv_shipment_proportion'] * greater_than_5['ltv_shipment_proportion_2']

        weightage_shipment = 0.25
        weightage_psp = 0.25
        weightage_loyalty = 0.25
        weightage_ltv = 0.25


        proportion_cols = ['shipment_proportion','psp_proportion','loyalty_proportion','ltv_shipment_proportion']
        weightage_cols = [weightage_shipment,weightage_psp,weightage_loyalty,weightage_ltv]


        TOTAL_SCORE = 5
        greater_than_5['score'] = 0
        for p, w in zip(proportion_cols, weightage_cols):
            greater_than_5['score'] += greater_than_5[p] * w * TOTAL_SCORE
            
        
        

        greater_than_5.loc[greater_than_5['last_5months_rto_percentage']>=0.80,'score']=greater_than_5\
                                                    [greater_than_5['last_5months_rto_percentage']>=0.80]['score']-1
        greater_than_5.loc[(greater_than_5['last_5months_rto_percentage']>0.4)&(greater_than_5['last_5months_rto_percentage']<0.80),'score']=greater_than_5\
                                                                    [(greater_than_5['last_5months_rto_percentage']>0.4)&(greater_than_5['last_5months_rto_percentage']<0.80)]['score']-0.5
        
        
        
        
        return greater_than_5

        
    def LT5_5months(less_than_5):

        less_than_5['shipment_proportion_1'] = less_than_5['last_5months_shipment'].apply(lambda x : 0.75 if x == 2 or x == 3 else(
                                                                                    1 if x == 4 or x==5 else(0)))

        less_than_5.loc[less_than_5['last_5months_recency'] <=10,'shipment_proportion_2'] = 1
        less_than_5.loc[(less_than_5['last_5months_recency'] <=20)&(less_than_5['last_5months_recency'] >10),'shipment_proportion_2'] = 0.8
        less_than_5.loc[(less_than_5['last_5months_recency'] <=30)&(less_than_5['last_5months_recency'] >20),'shipment_proportion_2'] = 0.6
        less_than_5.loc[less_than_5['last_5months_recency'] > 30,'shipment_proportion_2'] = 0.5

        less_than_5['shipment_proportion'] = less_than_5['shipment_proportion_1'] * less_than_5['shipment_proportion_2'] 


        less_than_5.loc[less_than_5['last_5months_pop'] > 0.70,'psp_proportion_2'] = 1
        less_than_5.loc[(less_than_5['last_5months_pop'] <=0.70)&(less_than_5['last_5months_pop'] >0.50),'psp_proportion_2'] = 0.75
        less_than_5.loc[less_than_5['last_5months_pop'] <=0.50,'psp_proportion_2'] = 0.5


        less_than_5['psp_proportion'] = less_than_5['shipment_proportion_1'] * less_than_5['psp_proportion_2'] 


        less_than_5['loyalty_proportion'] = less_than_5['last_5months_loyalty'].apply(lambda x : 1 if x >= 365 else(
                                                                0.8 if 180 < x < 365 else(
                                                                0.6 if 90 < x <= 180 else(0.5))))

        less_than_5.loc[less_than_5['last_5months_recency'] <=10,'loyalty_proportion_2'] = 1
        less_than_5.loc[(less_than_5['last_5months_recency'] <=20)&(less_than_5['last_5months_recency'] >10),'loyalty_proportion_2'] = 0.8
        less_than_5.loc[(less_than_5['last_5months_recency'] <=30)&(less_than_5['last_5months_recency'] >20),'loyalty_proportion_2'] = 0.6
        less_than_5.loc[less_than_5['last_5months_recency'] > 30,'loyalty_proportion_2'] = 0.5


        less_than_5['loyalty_proportion'] = less_than_5['loyalty_proportion'] * less_than_5['loyalty_proportion_2']


        less_than_5['ltv_shipment_proportion'] = less_than_5['last_5months_shipments_ltv'].apply(lambda x : 1 if x >= 50 else(
                                                                            0.8 if 20 <= x < 50 else(
                                                                            0.7 if 10 <= x < 20 else(
                                                                            0.6 if 0 < x < 10 else (0)))))


        less_than_5.loc[(less_than_5['last_5months_age'] < 90)&(less_than_5['last_5months_shipments_ltv'] < 50),'ltv_shipment_proportion_2'] = 1
        less_than_5.loc[(less_than_5['last_5months_age'] <=180)&(less_than_5['last_5months_age'] >90)&(less_than_5['last_5months_shipments_ltv'] < 50),'ltv_shipment_proportion_2'] = 0.8
        less_than_5.loc[(less_than_5['last_5months_age'] <=365)&(less_than_5['last_5months_age'] >180)&(less_than_5['last_5months_shipments_ltv'] < 50),'ltv_shipment_proportion_2'] = 0.6
        less_than_5.loc[(less_than_5['last_5months_age'] > 365)&(less_than_5['last_5months_shipments_ltv'] < 50),'ltv_shipment_proportion_2'] = 0.4


        less_than_5.loc[less_than_5['ltv_shipment_proportion_2'].isna(),'ltv_shipment_proportion_2']=1

        less_than_5['ltv_shipment_proportion'] = less_than_5['ltv_shipment_proportion'] * less_than_5['ltv_shipment_proportion_2']


        weightage_shipment = 0.25
        weightage_psp = 0.25
        weightage_loyalty = 0.25
        weightage_ltv = 0.25


        proportion_cols = ['shipment_proportion','psp_proportion','loyalty_proportion','ltv_shipment_proportion']
        weightage_cols = [weightage_shipment,weightage_psp,weightage_loyalty,weightage_ltv]


        TOTAL_SCORE = 5
        less_than_5['score'] = 0
        for p, w in zip(proportion_cols, weightage_cols):
            less_than_5['score'] += less_than_5[p] * w * TOTAL_SCORE





        less_than_5.loc[less_than_5['last_5months_rto_percentage']>=0.80,'score']=less_than_5\
                                                        [less_than_5['last_5months_rto_percentage']>=0.80]['score']-1
        less_than_5.loc[(less_than_5['last_5months_rto_percentage']>0.55)&(less_than_5['last_5months_rto_percentage']<0.80),'score']=less_than_5\
                                                        [(less_than_5['last_5months_rto_percentage']>0.55)&(less_than_5['last_5months_rto_percentage']<0.80)]['score']-0.5
        
        return less_than_5



    def rating_30days(data):
        greater_than_5 = GT5_30days(data[data['last_30_days_shipment']>5])
        less_than_5 = LT5_30days(data[data['last_30_days_shipment']<=5])


        greater_than_5.rename(columns = {"score":"score_30days"},inplace=True)
        less_than_5.rename(columns = {"score":"score_30days"},inplace=True)

        data = pd.concat([less_than_5,greater_than_5],axis=0)
        data.drop(columns = ['shipment_proportion_1', 'shipment_proportion_2', 'shipment_proportion',
                            'psp_proportion_2', 'psp_proportion', 'loyalty_proportion',
                            'loyalty_proportion_2', 'ltv_shipment_proportion',
                        'ltv_shipment_proportion_2', 'loyalty_proportion_1'],inplace=True)
        return data


    def rating_5months(data):
        greater_than_5 = GT5_5months(data[data['last_5months_shipment']>5])
        less_than_5 = LT5_5months(data[data['last_5months_shipment']<=5])

        greater_than_5.rename(columns = {"score":"score_5months"},inplace=True)
        less_than_5.rename(columns = {"score":"score_5months"},inplace=True)

        data2 = pd.concat([less_than_5,greater_than_5],axis=0)
        data2.drop(columns = ['shipment_proportion_1', 'shipment_proportion_2', 'shipment_proportion',
                            'psp_proportion_2', 'psp_proportion', 'loyalty_proportion',
                            'loyalty_proportion_2', 'ltv_shipment_proportion',
                            'ltv_shipment_proportion_2', 'loyalty_proportion_1'],inplace=True)

        data = data.merge(data2[['customer_phone','score_5months']], on='customer_phone',how='left')

        return data



    data = rating_30days(data)
    data = rating_5months(data)


    def final_score(data):
        if data.last_30_days_shipment > 5:
            return data['score_5months']*0.2 + data['score_30days']*0.8
        else:
            return data['score_5months']*0.1 + data['score_30days']*0.9


    data['final_score'] = data[['last_30_days_shipment','score_30days','score_5months']].apply(lambda x : final_score(x),axis=1)

    data.loc[data['final_score'].isna(),'final_score']=data[data['score_5months'].isna()]['score_30days']
    data.loc[data['last_30_days_shipment']==1,'final_score']=data[data['last_30_days_shipment']==1]['score_5months']
    data.loc[(data['last_30_days_shipment']==1)&(data['last_5months_shipment']==1),'final_score']=data[(data['last_30_days_shipment']==1)&(data['last_5months_shipment']==1)]['score_30days']
    data.loc[(data['last_30_days_shipment']==1)&(data['last_5months_shipment'].isna()),'final_score']=data[(data['last_30_days_shipment']==1)&(data['last_5months_shipment'].isna())]['score_30days']
    data.loc[(data['final_score']>=4)&(data['last_30_days_rto_percentage']>0.8),'final_score']=data[(data['final_score']>=4)&(data['last_30_days_rto_percentage']>0.8)]['final_score']-1


    data.loc[data['final_score']<0,'final_score'] = 0

    data['rating_category'] = data['final_score'].apply(lambda x : 'Good' if x >= 4 else(
                                                                'Bad' if x < 3 else('Fine')))


    l1 = 1000
    l2 = 2000
    
    def limit(data):
        if data.final_score <= 4.2 :
            x = min(data.last_30_days_value *0.10 , data.RPS*0.30)        
            if x < l1 :
                return x
            else:
                return l1
            
        elif data.final_score > 4.2 and data.final_score <= 4.4 :
            x = min(data.last_30_days_value * 0.10 , data.RPS*0.40)
            if x < l1 :
                return x
            else:
                return l1
        elif data.final_score > 4.4 and data.final_score <= 4.6 :
            x = min(data.last_30_days_value * 0.10 , data.RPS*0.50)
            if x < l1 :
                return x
            else:
                return l1
        elif data.final_score > 4.6 and data.final_score <= 4.8 :
            x = min(data.last_30_days_value * 0.15 , data.RPS*0.60)
            if x < l2 :
                return x
            else:
                return l2
        else :
            x = min(data.last_30_days_value * 0.15 , data.RPS*0.70)
            if x < l2 :
                return x
            else:
                return l2

    
    data['rps']= data['last_30_days_value']/data['last_30_days_shipment']
    data.loc[data['final_score']>=4 , 'limit'] = data[data['final_score']>=4][['final_score','last_30_days_value','rps']].apply(lambda x : limit(x), axis=1)



    data.rename(columns = {'final_score':'customer_rating','score_30days':'rating_30days','score_5months':'rating_5months'},inplace=True)
    bins = [-1,0,1,2,3,4,5]
    data['bins_rating'] = pd.cut(data['customer_rating'],bins)
    print(data.columns)
            
    


    return data





















    
