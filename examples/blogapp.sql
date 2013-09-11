--
-- PostgreSQL database dump
--

SET statement_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;

--
-- Name: plpgsql; Type: EXTENSION; Schema: -; Owner: 
--

CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA pg_catalog;


--
-- Name: EXTENSION plpgsql; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION plpgsql IS 'PL/pgSQL procedural language';


SET search_path = public, pg_catalog;

SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: author; Type: TABLE; Schema: public; Owner: dave; Tablespace: 
--

CREATE TABLE author (
    id integer NOT NULL,
    firstname character varying(50),
    lastname character varying(50),
    email character varying(75),
    biography text
);


ALTER TABLE public.author OWNER TO dave;

--
-- Name: author_id_seq; Type: SEQUENCE; Schema: public; Owner: dave
--

CREATE SEQUENCE author_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.author_id_seq OWNER TO dave;

--
-- Name: author_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dave
--

ALTER SEQUENCE author_id_seq OWNED BY author.id;


--
-- Name: blog; Type: TABLE; Schema: public; Owner: dave; Tablespace: 
--

CREATE TABLE blog (
    id integer NOT NULL,
    name character varying(100),
    description text
);


ALTER TABLE public.blog OWNER TO dave;

--
-- Name: blog_id_seq; Type: SEQUENCE; Schema: public; Owner: dave
--

CREATE SEQUENCE blog_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.blog_id_seq OWNER TO dave;

--
-- Name: blog_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dave
--

ALTER SEQUENCE blog_id_seq OWNED BY blog.id;


--
-- Name: entry; Type: TABLE; Schema: public; Owner: dave; Tablespace: 
--

CREATE TABLE entry (
    id integer NOT NULL,
    headline character varying(255),
    body_text text,
    pud_date date,
    mod_date date,
    comments integer,
    rating integer,
    blog_id integer
);


ALTER TABLE public.entry OWNER TO dave;

--
-- Name: entry_authors; Type: TABLE; Schema: public; Owner: dave; Tablespace: 
--

CREATE TABLE entry_authors (
    entry_id integer,
    author_id integer,
    id integer NOT NULL
);


ALTER TABLE public.entry_authors OWNER TO dave;

--
-- Name: entry_authors_id_seq; Type: SEQUENCE; Schema: public; Owner: dave
--

CREATE SEQUENCE entry_authors_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.entry_authors_id_seq OWNER TO dave;

--
-- Name: entry_authors_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dave
--

ALTER SEQUENCE entry_authors_id_seq OWNED BY entry_authors.id;


--
-- Name: entry_id_seq; Type: SEQUENCE; Schema: public; Owner: dave
--

CREATE SEQUENCE entry_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.entry_id_seq OWNER TO dave;

--
-- Name: entry_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dave
--

ALTER SEQUENCE entry_id_seq OWNED BY entry.id;


--
-- Name: vlogentry; Type: TABLE; Schema: public; Owner: dave; Tablespace: 
--

CREATE TABLE vlogentry (
    entry_id integer NOT NULL,
    video character varying(100)
);


ALTER TABLE public.vlogentry OWNER TO dave;

--
-- Name: id; Type: DEFAULT; Schema: public; Owner: dave
--

ALTER TABLE ONLY author ALTER COLUMN id SET DEFAULT nextval('author_id_seq'::regclass);


--
-- Name: id; Type: DEFAULT; Schema: public; Owner: dave
--

ALTER TABLE ONLY blog ALTER COLUMN id SET DEFAULT nextval('blog_id_seq'::regclass);


--
-- Name: id; Type: DEFAULT; Schema: public; Owner: dave
--

ALTER TABLE ONLY entry ALTER COLUMN id SET DEFAULT nextval('entry_id_seq'::regclass);


--
-- Name: id; Type: DEFAULT; Schema: public; Owner: dave
--

ALTER TABLE ONLY entry_authors ALTER COLUMN id SET DEFAULT nextval('entry_authors_id_seq'::regclass);


--
-- Data for Name: author; Type: TABLE DATA; Schema: public; Owner: dave
--

COPY author (id, firstname, lastname, email, biography) FROM stdin;
1	David	Krutký	david@deving.cz	Biografie autora David Krutký
2	Jan	Ondruch	honza@deving.cz	Biografie autora Jan Ondruch
\.


--
-- Name: author_id_seq; Type: SEQUENCE SET; Schema: public; Owner: dave
--

SELECT pg_catalog.setval('author_id_seq', 2, true);


--
-- Data for Name: blog; Type: TABLE DATA; Schema: public; Owner: dave
--

COPY blog (id, name, description) FROM stdin;
1	Peter's tool blog	Blog o nářadí
2	Devblog	Firemní blog firmy deving
\.


--
-- Name: blog_id_seq; Type: SEQUENCE SET; Schema: public; Owner: dave
--

SELECT pg_catalog.setval('blog_id_seq', 2, true);


--
-- Data for Name: entry; Type: TABLE DATA; Schema: public; Owner: dave
--

COPY entry (id, headline, body_text, pud_date, mod_date, comments, rating, blog_id) FROM stdin;
1	Příspěvek v blogu s nářadím	Obsah příspěvku obsahuje popis nářadí	2013-03-12	2013-05-05	10	0	1
2	Jak vybrat akušroubovák	Nechte si poradit serverem Hobby.cz, jak vybrat akušroubovák.	2013-04-01	2013-05-05	0	1	1
3	Veselé Velikonoce	Užijte si slunné dny a jarní pohodu	2013-03-30	2013-05-05	0	0	2
4	Knihovna pypg 0.1	Popis nově vytvořené knihovny pro komunikaci s databází.	2013-05-05	2013-05-05	3	15	2
\.


--
-- Data for Name: entry_authors; Type: TABLE DATA; Schema: public; Owner: dave
--

COPY entry_authors (entry_id, author_id, id) FROM stdin;
3	2	6
3	1	7
1	2	8
2	2	9
4	1	10
\.


--
-- Name: entry_authors_id_seq; Type: SEQUENCE SET; Schema: public; Owner: dave
--

SELECT pg_catalog.setval('entry_authors_id_seq', 10, true);


--
-- Name: entry_id_seq; Type: SEQUENCE SET; Schema: public; Owner: dave
--

SELECT pg_catalog.setval('entry_id_seq', 4, true);


--
-- Data for Name: vlogentry; Type: TABLE DATA; Schema: public; Owner: dave
--

COPY vlogentry (entry_id, video) FROM stdin;
\.


--
-- Name: author_email_key; Type: CONSTRAINT; Schema: public; Owner: dave; Tablespace: 
--

ALTER TABLE ONLY author
    ADD CONSTRAINT author_email_key UNIQUE (email);


--
-- Name: author_pkey; Type: CONSTRAINT; Schema: public; Owner: dave; Tablespace: 
--

ALTER TABLE ONLY author
    ADD CONSTRAINT author_pkey PRIMARY KEY (id);


--
-- Name: blog_pkey; Type: CONSTRAINT; Schema: public; Owner: dave; Tablespace: 
--

ALTER TABLE ONLY blog
    ADD CONSTRAINT blog_pkey PRIMARY KEY (id);


--
-- Name: entry_authors_pkey; Type: CONSTRAINT; Schema: public; Owner: dave; Tablespace: 
--

ALTER TABLE ONLY entry_authors
    ADD CONSTRAINT entry_authors_pkey PRIMARY KEY (id);


--
-- Name: entry_pkey; Type: CONSTRAINT; Schema: public; Owner: dave; Tablespace: 
--

ALTER TABLE ONLY entry
    ADD CONSTRAINT entry_pkey PRIMARY KEY (id);


--
-- Name: vlogentry_pkey; Type: CONSTRAINT; Schema: public; Owner: dave; Tablespace: 
--

ALTER TABLE ONLY vlogentry
    ADD CONSTRAINT vlogentry_pkey PRIMARY KEY (entry_id);


--
-- Name: blog_name_idx; Type: INDEX; Schema: public; Owner: dave; Tablespace: 
--

CREATE INDEX blog_name_idx ON blog USING btree (name);


--
-- Name: entry_authors_author_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dave
--

ALTER TABLE ONLY entry_authors
    ADD CONSTRAINT entry_authors_author_id_fkey FOREIGN KEY (author_id) REFERENCES author(id);


--
-- Name: entry_authors_entry_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dave
--

ALTER TABLE ONLY entry_authors
    ADD CONSTRAINT entry_authors_entry_id_fkey FOREIGN KEY (entry_id) REFERENCES entry(id);


--
-- Name: entry_blog_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dave
--

ALTER TABLE ONLY entry
    ADD CONSTRAINT entry_blog_id_fkey FOREIGN KEY (blog_id) REFERENCES blog(id);


--
-- Name: vlogentry_entry_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dave
--

ALTER TABLE ONLY vlogentry
    ADD CONSTRAINT vlogentry_entry_id_fkey FOREIGN KEY (entry_id) REFERENCES entry(id);


--
-- Name: public; Type: ACL; Schema: -; Owner: postgres
--

REVOKE ALL ON SCHEMA public FROM PUBLIC;
REVOKE ALL ON SCHEMA public FROM postgres;
GRANT ALL ON SCHEMA public TO postgres;
GRANT ALL ON SCHEMA public TO PUBLIC;


--
-- PostgreSQL database dump complete
--

