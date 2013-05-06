# -*- coding: utf-8 -*-

import psycopg2
from pypg import PyPg
import datetime

conn = psycopg2.connect("dbname=dbname user=user")
db = PyPg(conn, debug=True, strict=True)

def print_database_data():
    """Tato funkce ukazuje použití samostatných dotazů"""
    for blog in db.blog.select():
        print 'Blog: ', blog['name']
        print 'Přispěvky v blogu: '
        for entry in blog.entry.limit(10).select():
            print '\t', entry['headline']
            print '\tAutoři příspěvku: '
            for author_entry in entry.entry_authors.select():
                author = author_entry.author_id
                print '\t\t', author['firstname'], author['lastname']
                
def create_mview():
    """Tato funkce ukazuje vytvoření materializovaného pohledu a 
    práci s tímto pohledem."""
    db.create_mview('entry_author_view', db.entry.join(db.entry_authors)\
        .join(db.author, on=(db.author.id==db.entry_authors.author_id))\
        .select(db.entry.headline, db.author.firstname, db.author.lastname)\
        .order(db.entry.headline))
    for mview_row in db.entry_author_view.limit(1).select():
        print 'Příspěvek: ', mview_row['entry_headline'], 'Autor: ',\
                mview_row['author_firstname'], mview_row['author_lastname']
    
def drop_mview():
    """Tato funkce odstraní materializovaný pohled vytvořený 
    funkcí create_mview výše"""
    db.drop_mview('entry_author_view')
    
def fill_statistics():
    """Tato funkce zapne logování statistik a provede několik dotazů.
    Statistiky bude možné později využít pro funkci automatické dermalizace."""
    db.set_log(True)
    today = datetime.date.today()
    for entry in db.entry.select().limit(5).order(db.entry.headline):
        entry.update(mod_date=today)
    for i in range(5):
        blog = db.blog.insert_and_get(name='Nový blog', description='Popis nového blogu')[0]
        db.entry.insert(headline='Nový přispěvek', body_text='Text přispěvku', pud_date=today,\
                        mod_date=today, comments=0, rating=0, blog_id=blog['id'])
    for entry in db.entry.select().limit(5).order_desc(db.entry.id):
        entry.delete()
    for i in range(30):
        db.entry.join(db.blog).select(db.entry.headline, db.blog.name).order(db.blog.name)[0]
    for i in range(30):
        db.entry.join(db.entry_authors).join(db.author, on=db.author.id == db.entry_authors.author_id)\
        .select(db.entry.headline, db.author.firstname)[0]
        
def run_denormalization():
    """Tato funkce spustí automatickou denormalizaci. Měla by být spouštěna až poté
    co je spuštěna funkce fill_statistics."""
    db.set_debug(False)
    db.start_denormalization()
    db.set_debug(True)
         
if __name__ == "__main__":
    print_database_data()
    #create_mview()
    #drop_mview()
    #fill_statistics()
    #run_denormalization()
