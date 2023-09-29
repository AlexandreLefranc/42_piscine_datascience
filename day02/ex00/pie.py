import pandas as pd
from sqlalchemy import create_engine
import matplotlib.pyplot as plt

def main():    
    engine = create_engine('postgresql+psycopg2://alefranc:mysecretpassword@localhost:5432/piscineds')

    sql = "SELECT event_type FROM customers;"

    with engine.connect() as conn, conn.begin():
        data = pd.read_sql_query(sql, conn)
        
        
    total = data['event_type'].size
    counts = data['event_type'].value_counts().to_list()
    names = data['event_type'].value_counts().index.to_list()
    
    
        
    
    fig, ax = plt.subplot()
    ax.pie(counts, labels=names, autopct='%1.1f%%')
    
    
    

if __name__ == "__main__":
    main()
