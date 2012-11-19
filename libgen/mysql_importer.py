import logging
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import mapper, sessionmaker
from solr import SolrInterface

MYSQL_DB_HOST = "localhost"
MYSQL_DB_NAME = "bookwarrior"
MYSQL_DB_USERNAME = "root"
MYSQL_DB_PASSWORD = "root"

SOLR_ENDPOINTS = { "en" : "http://localhost:8983/solr/en/" }
SOLR_DEFAULT_ENDPOINT = "en"


class Book(object):
    """
    ORM Class to represent a book
    """
    pass

def get_db_session():
    engine = create_engine("mysql://%s:%s@%s/%s?charset=utf8" % (MYSQL_DB_USERNAME, MYSQL_DB_PASSWORD, MYSQL_DB_HOST, MYSQL_DB_NAME, ), convert_unicode=True, echo=True)
    metadata = MetaData(engine)
    books = Table("updated", metadata, autoload=True)
    mapper(Book, books)

    Session = sessionmaker(bind=engine)
    session = Session()
    return session

def do_import():
    session = get_db_session()

    # Create solr session instance
    total_books = session.query(Book.ID).count()
    solr = SolrInterface(SOLR_ENDPOINTS, SOLR_DEFAULT_ENDPOINT)
    solr.deleteAll()

    counter = 0
    for book in session.query(Book.MD5, 
                              Book.Title, 
                              Book.Author, 
                              Book.Language, 
                              Book.VolumeInfo, 
                              Book.Series,
                              Book.Pages,
                              Book.Periodical, 
                              Book.Publisher, 
                              Book.Topic, 
                              Book.Year, 
                              Book.OpenLibraryID,
                              Book.Filename,
                              Book.Extension,
                              Book.Filesize,
                              Book.Coverurl):

        solr_document = { "id" : book.MD5, "title" : book.Title, "language" : book.Language, "extension": book.Extension }

        authors = [author.strip() for author in book.Author.split(",") if len(author.strip()) > 0]
        if authors:
            solr_document["author"] = authors

        if book.Year and book.Year.isdigit():
            solr_document["year"] = book.Year

        if book.Pages and book.Pages.isdigit():
            solr_document["pages"] = book.Pages

        if book.OpenLibraryID:
            solr_document["openlibrary_id"] = book.OpenLibraryID

        if book.Filesize:
            solr_document["filesize"] = book.Filesize

        solr_document["full_text"] = " ".join([book.VolumeInfo, book.Series, book.Periodical, book.Publisher, book.Topic])
        solr_document["file_url"] = book.Filename
        solr_document["cover_url"] = book.Coverurl

        solr.add(solr_document)
        counter += 1
        print "%s/%s" % (counter, total_books)

    solr.commit()
    solr.optimize()

if __name__ == "__main__":
    logging.basicConfig()
    do_import()
