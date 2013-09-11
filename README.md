pypg
====

PostgreSQL data access layer in Python. The application was inspired by PHP library [NotORM](http://www.notorm.com/).

It includes functionality as:

* Simulation of materialized views
* Improvement plans for database structures based on benchmarks and database usage
* Smart querying of related records by listing ID's (similar as in NotORM)

The application was developed for my [Master thesis](http://theses.cz/id/sgb68f).

Instalace
--------------------------

Knihovna je distribuována ve formě balíku programovacího jazyka Python. Pro spuštění knihovny stačí, 
když bude celý balík knihovny v cestě vykonávání programu `PYTHONPATH`.
Požadavky na spuštění knihovny jsou:
* SŘBD PostgreSQL minimální verze 8.4
* Python 2.7
* Databázový ovladač psycopg2 2.4.5

Knihovna byla testována s verzemi výše uvedených technologií na operačním systému Ubuntu 12.4. 
Provoz na operačním systému Windows otestován nebyl, knihovna by však měla být schopná na tomto systému pracovat bez větších problémů.
Pro funkci automatické denormalizace je také potřeba, aby byla přes příkazovou řádku dostupná funkce PostgreSQL `psql` a `pg_dump`.

Inicializace
------------------------------

Pro inicializaci knihovny je potřeba nejdříve vytvořit spojení do databáze pomocí databázového ovladače `psycopg2` a toto spojení předat třídě `PyPg` z balíku `pypg`.

```python
>>> import psycopg2
>>> from pypg import PyPg
>>> conn = psycopg2("dbname=databasename user=databaseuser")
>>> db = PyPg(conn)
```

Při inicializaci třídy `PyPg` je možné také předat nepovinné parametry, které určují nastavení knihovny. 
Mezi tyto parametry patří:
* __debug__ Tento parametr je typu `boolean` a určuje, zda bude knihovna vypisovat prováděné SQL dotazy, a zda bude pomocí metadat získaných introspekcí napovídat při vytváření kódu nebo při vypisování výjimek. Implicitně je nastaven na `False`.
* __logger__ Tento parametr je typu `logging.Logger`. Pro vypisování prováděných SQL dotazů knihovna používá standardní logování jazyka Python a implicitně se dotazy vypisují na standardní výstup. Nastavením tohoto parametru se při logování dotazů použije předaná instance třídy `logging.Logger`.
* __strict__ Tímto parametrem lze určit, zda se pro sestavování SQL dotazů použijí metadata získaná introspekcí z informačního schématu PostgreSQL. Také tento parametr určuje, zda se budou dotazy sestavené pomocí API knihovny validovat. Pokud je nastaven na `False`, pro sestavování dotazů se využije sada pravidle v podobě třídy `pypg.structure.Naming`. Implicitně sada pravidel určuje název primárního klíče jako `id`, a název vazebního atributu na tabulku `tabulka` jako `tabulka_id`. Implicitně je parametr `strict` nastaven na `False`.
* __naming__ Tímto parametrem lze předat vlastní sadu pravidel v podobě instance třídy, která dědí z třídy `pypg.structure.Naming`. 
* __log__ Tento parametr určuje, zda se budou shromažďovat statistiky prováděných SQL dotazů. Tyto statistiky se využívají při funkci automatické denormalizace. Jakmile je tento parametr nastaven na `True`, provede se záloha aktuální databáze a vytvoří se soubor `statistics.log`, do kterého poté knihovna ukládá statistiky ve formátu JSON. Oba tyto soubory jsou umístěny v balíku knihovny do složky `log/`. Implicitně je tento parametr nastaven na `False`

Všechny tyto parametry lze nastavit při inicializaci třídy `PyPg` nebo pomocí metod instance této třídy.
```python
>>> db = PyPg(conn, strict=True)
>>> db.set_log(False)
>>> db.set_debug(True)
```

Tvoření dotazů
------------------------------------------

Přes instanci třídy `PyPg` se lze dostat k objektům představující databázové tabulky. Pokud je povolena introspekce, je možné získat nápovědu o všech tabulkách v databázi. Také je při přístupu k neexistující tabulce vyvolána výjimka.

```python
>>> db.blog
<pypg.table.Table at 0x993a5cc>
>>> db.notexistingtable
PyPgException: 'No table "notexistingtable" in database. Choices are: blog, entry, entry_authors, vlogentry, author'
```

Přes objekt třídy `Table` je pak možné se dostat k objektům třídy `Column` představující databázové sloupce. Opět, pokud je povolena introspekce, je chování podobné jako při přístupu k tabulkám.

```python
>>> db.blog.name
<pypg.column.Column at 0x9b857ac>
>>> db.blog.notexistingcolumn
PyPgException: 'Column "notexistingcolumn" is not a valid column in table "blog". Choices are: name, description, id'
```

Objekt třídy `Table` poskytuje metody pro tvoření SQL dotazů. Mezi tyto metody patří `limit`, `order`, `order_desc`, `where`, `join` a `select`.

* __limit__ Tato metoda přijímá jediný parametr, který musí být typu `integer`, nebo převeditelný na typ `integer`.

```python
>>> db.blog.limit(10).select()[0]
SELECT * FROM "blog"    LIMIT 10
```
* __order__ Metoda `order` přijímá jenom jediný parametr, kterým lze data seřadit. Tento parametr musí být typu `Column`. 

```python
>>> db.blog.order(db.blog.name).select()[0]
SELECT * FROM "blog"   ORDER BY blog.name
```
* __order_desc__ Metoda se chová stejně jako `order`, ale při jejím použití jsou výsledná data seřazena sestupně.
* __where__ Metoda `where` přijímá neomezený počet parametrů. Tyto parametry musí být podmínky sestavené pomocí instancí třídy `Column`. Všechny tyto podmínky jsou spojeny pomocí SQL klauzule `AND`.

```python
>>> db.entry.where(db.entry.rating>0, db.entry.comments==0).select()[0]
SELECT * FROM "entry"  WHERE entry.rating > 0 AND entry.comments = 0
```

* __join__ Tato metoda vytváří dotaz pomocí spojení tabulek. Prvním parametrem metody je instance třídy `Table`. Druhý parametr určuje podmínku spojení tabulek a je tvořen pomocí instancí třídy `Column`. Pokud je povolena introspekce, podmínka je implicitně zjištěna z metadat databáze. 

```python
>>> db.blog.join(db.entry).select()[0]
SELECT * FROM "blog" JOIN "entry" ON entry.blog_id = blog.id
...
>>> db.blog.join(db.entry, on=db.blog.id==db.entry.id).select()[0]
SELECT * FROM "blog" JOIN "entry" ON blog.id = entry.id
```
* __select__ Metoda `select` přijímá neomezené množství nepovinných parametrů. Tyto parametry určují databázové sloupce, na které bude dotazováno a musí být typu `Column`.

```python
>>> db.entry.select(db.entry.headline, db.entry.rating)[0]
SELECT entry.headline, entry.rating, entry.id FROM "entry"
```

Při vytváření dotazů musí být vždy využita metoda `select`. Podle této metody knihovna pozná, že již může provést SQL dotaz. Knihovna však provedení dotazu odkládá do chvíle, kdy je to nezbytně nutné. Lze proto jednotlivé metody pro dotazování řetězit.

```python
>>> query = db.entry.order(db.entry.headline)
>>> query = query.limit(10)
>>> query = query.select(db.entry.headline)
>>> query[0]
SELECT entry.headline, entry.id FROM "entry"   ORDER BY entry.headline LIMIT 1
<pypg.row.Row at 0xa28734c>
```

Při tvoření dotazů nezáleží na pořadí jednotlivých metod.

```python
>>> db.blog.limit(10).select(db.blog.name).order(db.blog.name)[0]
SELECT blog.name, blog.id FROM "blog"   ORDER BY blog.id LIMIT 10
```

Pro zadání vlastně vytvořeného SQL lze využít třídu `pypg.query.Query`.

```python
>>> from pypg.query import Query
>>> Query().execute_and_fetch('SELECT * FROM "blog" ORDER BY blog.name')
SELECT * FROM "blog" ORDER BY blog.name
[[2, 'Devblog', 'Firemn\xc3\xad blog firmy deving'],
[1, "Peter's tool blog", 'Blog o n\xc3\xa1\xc5\x99ad\xc3\xad']]
```

Pro kódování znaků se využívá UTF8. Pro vypsání dat z databáze na příkazové řádce ve správném tvaru se použije funkce `print`.

```python
>>> for row in db.blog.select():
...     print row['name'], row['description']
SELECT * FROM "blog"
Peters tool blog Blog o nářadí
Devblog Firemní blog firmy deving
```

Tvoření podmínek dotazů
-------------------------

Třída `Column` umožňuje vytvářet při sestavování dotazů podmínky pomocí svých instancí. Mezi tyto metody patří `__eq__`, `__ne__`, `__gt__`, `__lt__`, `like` a `in_`.

**\_\_eq\_\_**
```python
>>> print db.entry.rating == 0
entry.rating = 0
```
**\_\_ne\_\_**
```python
>>> print db.entry.rating != 0
entry.rating <> 0
```
**\_\_gt\_\_**
```python
>>> print db.entry.rating > 0
entry.rating > 0
```
**\_\_lt\_\_**
```python
>>> print db.entry.rating < 0
entry.rating < 0
```
__like__
```python
>>> print db.blog.name.like("%blog")
blog.name LIKE %blog
```
**in\_**
```python
>>> print db.blog.id.in_([1,2,3])
blog.id IN (1, 2, 3)
```

Podmínky lze vytvořit i porovnáním dvou instancí třídy `Column`.
```python
>>> print db.entry.blog_id == db.blog.id
entry.blog_id = blog.id
```
Všechny proměnné použité v podmínce nebo i v metodách používaných pro sestavování dotazu jsou zpracovány tak, aby nedošlo k SQL Injection. 

Tvoření dotazů pro aktualizaci dat
-----------------------------------

Instance třídy `Table` umožňuje vytvořit i SQL dotazy, které aktualizují data v databázi. Mezi tyto metody patři `insert`, `insert_and_get`, `update`, `update_and_get` a `delete`.

* __insert__ Metoda se používá pro vytvoření jednoho řádku v databázi. Přijímá neomezený počet parametrů, jejichž názvy se musí shodovat s názvy atributů tabulky.

```python
>>> db.blog.insert(name="New blog", description="New blog description")
INSERT INTO "blog" ("name", "description") VALUES ('New blog', 'New blog description')
```
* __insert_and_get__ Tato metoda vytvoří záznam v databázi a zároveň vrátí všechny hodnoty zpět z databáze i s nově vygenerovanými nebo pozměněnými hodnotami při uložení. Hodnoty jsou vráceny v instanci třídy `ResultSet`, jelikož se v budoucnu počítá s možností uložení více záznamu najednou.

```python
>>> db.blog.insert_and_get(name="New blog", description="New blog description")
INSERT INTO "blog" ("name", "description") VALUES ('New blog', 'New blog description') RETURNING *
<pypg.resultset.ResultSet at 0xa287d4c>
```
* __update__ Metoda pro aktualizaci záznamů tabulky. Dotaz lze vytvořit s využitím metody `where`. Metoda `update` však musí být vždy v řetězení dotazů na konci. Přijímá neomezený počet parametrů, jejichž názvy se musí shodovat s názvy atributů tabulky.

```python
>>> db.blog.where(db.blog.id==1).update(name="Peters tool blog")
UPDATE "blog" SET "name"='Peters tool blog' WHERE blog.id = 1
```
* __update_and_get__ Tato metoda má stejnou podobu jako metoda `update`. Vrací však z databáze aktualizované záznamy. 

```python
>>> db.blog.where(db.blog.id==1).update_and_get(name="Peters tool blog")
UPDATE "blog" SET "name"='Peters tool blog' WHERE blog.id = 1 RETURNING *
<pypg.resultset.ResultSet at 0x9a524ac>
```
* __delete__ Metoda pro mazání záznamů v databázové tabulce. Lze vytvořit s využitím metody `where`. Metoda `delete` však musí být vždy v řetězení dotazů na konci.

```python
>>> db.blog.where(3==db.blog.id).delete()
DELETE FROM "blog" WHERE blog.id = 3
```

Výsledky dotazu
------------------

Metody `select`, `insert_and_get` a `update_and_get` vrací z databáze záznamy v podobě instance třídy `ResultSet`. Tato třída se chová podobně jako objekt pole. Je možné tedy výsledky iterovat nebo k nim přistoupit pomocí identifikátoru. Jednotlivé záznamy jsou pak ve formátu instancí třídy `Row`.

```python
>>> query = db.blog.select()
>>> for row in query:
...     print row['name'], row['description']
SELECT * FROM "blog"
Peters tool blog Blog o nářadí
Devblog Firemní blog firmy deving
>>> query[0]
<pypg.row.Row at 0x9a5232c>
```

S instancí třídy `Row` lze pak dále pracovat. Tato instance uchovává data ve formátu klíč-hodnota. Pro získání hodnoty daného atributu se přistupuje přes název tohoto atributu. Přes klíč lze nastavit záznamu i novou hodnotu.
```python
>>> row = query[1]
>>> row['description'] = "Blog description"
```

Instance poskytuje metody `update` a `delete`. Instance si pamatuje, které atributy byly přenastaveny a při zavolání metody update tyto změny uloží do databáze. Tato metoda přijímá také neomezený počet parametrů, jejichž názvy se musí shodovat s názvy atributů tabulky.
```python
>>> row.update()
UPDATE "blog" SET "description"='Blog description' WHERE "id"=2
>>> row.update(name="Blog name")
UPDATE "blog" SET "name"='Blog name' WHERE "id"=2
```

Pokud je záznam smazán metodou `delete`, instance zůstane v paměti, ale nelze s ní již dále pracovat.
```python
>>> row.delete()
DELETE FROM "blog" WHERE "id"=2
>>> row.update(name="Blog name")
PyPgException: 'This row was deleted.'
```

Relační záznamy
-----------------

Přes instanci třídy `Row` lze přistupovat k relačním záznamům. Pokud je přistupováno k relaci N:1, používá se atribut jehož název je shodný s názvem cizího klíče. Pokud je přistupováno k relaci 1:N, používá se atribut jehož název je shodný s názvem tabulky relačních objektů. Relaci 1:N lze dále filtrovat podle stejnými pravidly jako při tvoření dotazů.

```python
>>> row = db.entry.limit(1).select()[0]
SELECT * FROM "entry"    LIMIT 1
>>> row.blog_id['name']
SELECT * FROM "blog"  WHERE "id"=1
'Peters tool blog'
>>> for ea row.entry_authors.select():
...     print ea.author_id['firstname']
SELECT * FROM "entry_authors"  WHERE "entry_id"=1   
SELECT * FROM "author"  WHERE author.id IN (2)  
Jan
```

Funkce urychlující čtení dat z databáze
------------------------------------------

Při cyklování výsledky dotazu a při přistupování k relacím se vytváří efektivní dotazy, které nezatěžují databázi. Pro každou tabulku použitou v cyklu se vždy vytvoří pouze jeden dotaz.

Funkce knihovny pypg umožňuje vytvořit v databázi struktury, které simulují materializovaný pohled. Pro vytvoření se používá metoda `create_mview` instance třídy `PyPg`. Tato metoda přijímá jako první parametr název materializovaného pohledu a jako druhý parametr dotaz vytvoření pomocí API knihovny. Pomocí metody `drop_mview` je z databáze materializovaný pohled odstraněn. Tato operace by měla být prováděna mimo běh aplikace.

```python
>>> db.create_mview('blog_entry', db.blog.join(db.entry).select())
Materialized view blog_entry created successfully
>>> db.blog_entry.select()[0]
SELECT * FROM "blog_entry"   
<pypg.row.Row at 0x9f7a5ac>
>>> db.drop_mview('blog_entry')
Materialized view blog_entry dropped successfully
```

Pro funkci automatické denormalizace je nejdříve nutné zapnout logování statistik pomocí funkce `set_log` instance třídy `PyPg`. Poté knihovna loguje statistiky každého prováděného dotazu. Automatická denormalizace se pak spouští metodou `start_denormalization`. Tato funkce vyžaduje, aby byly přes příkazový řádek dostupné funkce `psql` a `pg_dump` SŘBD PostgreSQL. Při spuštění bude na databázovém serveru vytvořena testovací databáze, která bude testována a zatěžována. 

Vytvoření vlastní sady pravidel
--------------------------------------------------------

K vytvoření vlastních pravidel použitých při tvoření dotazů je potřeba podědit z třídy `pypg.structure.Naming` a předefinovat její metody.
```python
from pypg.structure import Naming
class CustomNaming(Naming):
    def get_pk_naming(self, table):
        return "id"

    def get_fk_naming(self, table, foreign_table):
        return "%s_id" % foreign_table

    def match_fk_naming(self, table, attr):
        return attr.endswith("_id")

    def get_fk_column(self, table, foreign_key):
        return foreign_key.rstrip("_id")
```
Metoda `get_pk_naming` přijímá jako parametr název tabulky a měla by vracet název primárního klíče této tabulky.

Metoda `get_fk_naming` přijíma parametry, které představují název tabulky a název relační tabulky. Metoda by měla vracet název cizího klíče pro tuto relaci.

Metoda `match_fk_naming` přijíma jako parametry název tabulky a název atributu. Metoda by měla zjistit, zda je tento atribut cizím klíčem a vrátit výsledek.

Metoda `get_fk_column` přijímá název tabulky a název cizího klíče, který tato tabulka obsahuje. Metoda by měla název relační tabulky, na kterou tento cizí klíč odkazuje.

Instanci této třídy je pak potřeba předat do nastavení knihovny.
```python
>>> db.set_naming(CustomNaming())
```
