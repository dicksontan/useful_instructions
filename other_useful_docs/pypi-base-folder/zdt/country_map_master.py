class country_map_class():

    def __init__(self, add_sales_org: list = []):

        self.country_map={
'BN': {'country_id':1,'begru_salesorg': {('ZPBN',1300)}, 'plant': 13, 'currency': 'BND', 'begru': {'ZPBN'}},
'HK': {'country_id':2,'begru_salesorg':{('ZPHK',1700),('ZPHK',1708),('ZPHK',1750)}, 'plant': 17, 'currency': 'HKD', 'begru' : {'ZPHK'}},
'MO': {'country_id':3,'begru_salesorg':{('ZPMO',1703)}, 'plant': 17, 'currency': 'MOP', 'begru' : {'ZPMO'}},
'MY': {'country_id':4,'begru_salesorg':{('ZPMY',2001)}, 'plant': 20, 'currency': 'MYR', 'begru' : {'ZPMY'}},
'SG': {'country_id':5,'begru_salesorg': {('ZPSG',2601)}, 'plant': 26, 'currency': 'SGD', 'begru' : {'ZPSG'}},
'TW': {'country_id':6,'begru_salesorg': {('ZPTW',2800),('ZPTW',2801),('ZPTW',2802),('ZPTW',2803),('ZPTW',2804),('ZPTW',2805)}, 'plant': 28, 'currency': 'TWD', 'begru' : {'ZPTW'}},
'TH': {'country_id':7,'begru_salesorg': {('ZPTH',2900),('ZPTH',2902)}, 'plant': 29, 'currency': 'THB', 'begru' : {'ZPTH'}},
'VN': {'country_id':8,'begru_salesorg': {('ZPVN',3000),('ZPVN',3001),('ZPVN',3050),('ZPVN',3070),('ZPVN',3072),('ZPVN',3090)}, 'plant': 30, 'currency': 'VND', 'begru' : {'ZPVN'}},
'KR': {'country_id':9,'begru_salesorg': {('ZPKR',3101),('ZPKR',3102),('ZPKR',3105),('ZPKR',3150),('ZPKR',3151),('ZPKR',3152),('ZPKR',3153)}, 'plant': 31, 'currency': 'KRW', 'begru' : {'ZPKR'}},
'MM': {'country_id':10,'begru_salesorg': {('ZPMM',2200),('ZPMM',2201),('ZPMM',2203),('ZPMM',2250)}, 'plant': 22, 'currency': 'MMK', 'begru' : {'ZPMM'}},
'KH': {'country_id':11,'begru_salesorg': {('ZPKH',1500),('APC',1501)}, 'plant': 15, 'currency': 'KHR', 'begru' : {'ZPKH','APC'}},
'PH': {'country_id':12,'begru_salesorg':{('ZPC',2500), ('MDI',2501), ('ISPI',2504)}, 'plant': 25, 'currency': 'PHP', 'begru' : {'ZPC','MDI','ISPI'}},
'ID': {'country_id':15,'begru_salesorg':{('ZPID',1900)}, 'plant': 19, 'currency': 'IDR', 'begru': {'ZPID'}},
'AL': {'country_id':99,'begru_salesorg':{('ZPAL',9999)}, 'plant': 99, 'currency': 'ALL', 'begru': {'ZPAL'}},
}

        #add sales_org into country_map_class
        #sample input: [('TH',('ZPTH',2911)), ('SG', ('ZPSG',100))]
        if add_sales_org:
            for tup in add_sales_org:
                self.country_map[tup[0]]['begru_salesorg'].add(tup[1])
                
