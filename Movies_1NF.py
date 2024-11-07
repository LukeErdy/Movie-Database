import pymysql
import sys
import pandas
import json
from prettytable import PrettyTable

YOUR_PASSWORD_HERE = ''

def createTables(conn, cur):
	cur.execute("DROP TABLE IF EXISTS MoviesGenres")
	cur.execute("DROP TABLE IF EXISTS MoviesKeywords")
	cur.execute("DROP TABLE IF EXISTS MoviesCompanies")
	cur.execute("DROP TABLE IF EXISTS MoviesCountries")
	cur.execute("DROP TABLE IF EXISTS MoviesLanguages")
	cur.execute("DROP TABLE IF EXISTS Movies")
	cur.execute("DROP TABLE IF EXISTS Genres")
	cur.execute("DROP TABLE IF EXISTS Keywords")
	cur.execute("DROP TABLE IF EXISTS ProductionCompanies")
	cur.execute("DROP TABLE IF EXISTS ProductionCountries")
	cur.execute("DROP TABLE IF EXISTS SpokenLanguages")

	sql = """CREATE TABLE Movies(
		budget int,
		homepage varchar(256),
		id int not null,
		original_language varchar(2),
		original_title varchar(128),
		overview varchar(1024),
		popularity float(23),
		release_date varchar(10),
		revenue bigint,
		runtime int,
		status varchar(16),
		tagline varchar(256),
		title varchar(128),
		vote_average float(23),
		vote_count int,
		primary key(id));
	"""
	cur.execute(sql)
	sql = """CREATE TABLE Genres(
		id int not null,
		name varchar(32),
		primary key(id));
	"""
	cur.execute(sql)
	sql = """CREATE TABLE Keywords(
		id int not null,
		name varchar(32),
		primary key(id));
	"""
	cur.execute(sql)
	sql = """CREATE TABLE ProductionCompanies(
		name varchar(64),
		id int not null,
		primary key(id));
	"""
	cur.execute(sql)
	sql = """CREATE TABLE ProductionCountries(
		iso_3166_1 varchar(2) not null,
		name varchar(32),
		primary key(iso_3166_1));
	"""
	cur.execute(sql)
	sql = """CREATE TABLE SpokenLanguages(
		iso_639_1 varchar(2) not null,
		name varchar(32),
		primary key(iso_639_1));
	"""
	cur.execute(sql)
	sql = """CREATE TABLE MoviesGenres(
		id_relationship int,
		id_movie int,
		id_genre int,
		foreign key(id_movie) references Movies(id),
		foreign key(id_genre) references Genres(id));
	"""
	cur.execute(sql)
	sql = """CREATE TABLE MoviesKeywords(
		id_relationship int,
		id_movie int,
		id_keyword int,
		foreign key(id_movie) references Movies(id),
		foreign key(id_keyword) references Keywords(id));
	"""
	cur.execute(sql)
	sql = """CREATE TABLE MoviesCompanies(
		id_relationship int,
		id_movie int,
		id_company int,
		foreign key(id_movie) references Movies(id),
		foreign key(id_company) references ProductionCompanies(id));
	"""
	cur.execute(sql)
	sql = """CREATE TABLE MoviesCountries(
		id_relationship int,
		id_movie int,
		id_country varchar(2),
		foreign key(id_movie) references Movies(id),
		foreign key(id_country) references ProductionCountries(iso_3166_1));
	"""
	cur.execute(sql)
	sql = """CREATE TABLE MoviesLanguages(
		id_relationship int,
		id_movie int,
		id_language varchar(2),
		foreign key(id_movie) references Movies(id),
		foreign key(id_language) references SpokenLanguages(iso_639_1));
	"""
	cur.execute(sql)
	conn.commit()

def readAndParse(conn, cur, fileName):
	df = pandas.read_csv(fileName)
	df = df.fillna(float(-1))

	# all attributes as lists
	size = len(df)
	budgets = list(df["budget"])
	homepages = list(df["homepage"])
	ids = list(df["id"])
	original_languages = list(df["original_language"])
	original_titles = list(df["original_title"])
	overviews = list(df["overview"])
	popularities = list(df["popularity"])
	release_dates = list(df["release_date"])
	revenues = list(df["revenue"])
	runtimes = list(df["runtime"])
	statuses = list(df["status"])
	taglines = list(df["tagline"])
	titles = list(df["title"])
	vote_averages = list(df["vote_average"])
	vote_counts = list(df["vote_count"])
	# non-atomic attributes
	genres = list(df["genres"])
	keywords = list(df["keywords"])
	production_companies = list(df["production_companies"])
	production_countries = list(df["production_countries"])
	spoken_languages = list(df["spoken_languages"])

	# enter atomic movie data
	data = ((budgets[i], homepages[i], ids[i], original_languages[i], original_titles[i], overviews[i], popularities[i], release_dates[i], revenues[i], runtimes[i], statuses[i], taglines[i], titles[i], vote_averages[i], vote_counts[i]) for i in range(size))
	sql = "INSERT INTO Movies(budget, homepage, id, original_language, original_title, overview, popularity, release_date, revenue, runtime, status, tagline, title, vote_average, vote_count) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
	cur.executemany(sql, data)

	# enter non-atomic attributes
	moviesGenres_id = 1
	moviesKeywords_id = 1
	moviesCompanies_id = 1
	moviesCountries_id = 1
	moviesLanguages_id = 1
	MG = []
	MK = []
	MC0 = []
	MC1 = []
	ML = []
	genresStatements = []
	keywordsStatements = []
	companiesStatements = []
	countriesStatements = []
	languagesStatements = []
	for i in range(size):
		# build tuples for non-atomic attributes
		genresList = json.loads(genres[i])
		genresParsed = [(item["id"], item["name"]) for item in genresList]
		genresStatements += genresParsed
		keywordsList = json.loads(keywords[i])
		keywordsParsed = [(item["id"], item["name"]) for item in keywordsList]
		keywordsStatements += keywordsParsed
		companiesList = json.loads(production_companies[i])
		companiesParsed = [(item["name"], item["id"]) for item in companiesList]
		companiesStatements += companiesParsed
		countriesList = json.loads(production_countries[i])
		countriesParsed = [(item["iso_3166_1"], item["name"]) for item in countriesList]
		countriesStatements += countriesParsed
		languagesList = json.loads(spoken_languages[i])
		languagesParsed = [(item["iso_639_1"], item["name"]) for item in languagesList]
		languagesStatements += languagesParsed

		# link movies with non-atomic attributes
		for genreTuple in genresParsed:
			MG.append((moviesGenres_id, ids[i], genreTuple[0]))
			moviesGenres_id += 1
		for keywordTuple in keywordsParsed:
			MK.append((moviesKeywords_id, ids[i], keywordTuple[0]))
			moviesKeywords_id += 1
		for companyTuple in companiesParsed:
			MC0.append((moviesCompanies_id, ids[i], companyTuple[1]))
			moviesCompanies_id += 1
		for countryTuple in countriesParsed:
			MC1.append((moviesCountries_id, ids[i], countryTuple[0]))
			moviesCountries_id += 1
		for languageTuple in languagesParsed:
			ML.append((moviesLanguages_id, ids[i], languageTuple[0]))
			moviesLanguages_id += 1

	# insert all non-atomic attributes
	sql = "INSERT IGNORE INTO Genres(id, name) VALUES (%s, %s)"
	cur.executemany(sql, genresStatements)
	sql = "INSERT IGNORE INTO Keywords(id, name) VALUES (%s, %s)"
	cur.executemany(sql, keywordsStatements)
	sql = "INSERT IGNORE INTO ProductionCompanies(name, id) VALUES (%s, %s)"
	cur.executemany(sql, companiesStatements)
	sql = "INSERT IGNORE INTO ProductionCountries(iso_3166_1, name) VALUES (%s, %s)"
	cur.executemany(sql, countriesStatements)
	sql = "INSERT IGNORE INTO SpokenLanguages(iso_639_1, name) VALUES (%s, %s)"
	cur.executemany(sql, languagesStatements)

	# insert all movie/non-atomic attribute relations
	sql = "INSERT INTO MoviesGenres(id_relationship, id_movie, id_genre) VALUES (%s, %s, %s)"
	cur.executemany(sql, MG)
	sql = "INSERT INTO MoviesKeywords(id_relationship, id_movie, id_keyword) VALUES (%s, %s, %s)"
	cur.executemany(sql, MK)
	sql = "INSERT INTO MoviesCompanies(id_relationship, id_movie, id_company) VALUES (%s, %s, %s)"
	cur.executemany(sql, MC0)
	sql = "INSERT INTO MoviesCountries(id_relationship, id_movie, id_country) VALUES (%s, %s, %s)"
	cur.executemany(sql, MC1)
	sql = "INSERT INTO MoviesLanguages(id_relationship, id_movie, id_language) VALUES (%s, %s, %s)"
	cur.executemany(sql, ML)
	conn.commit()

def query1(cur):
	# average budget of all movies
	sql = "SELECT AVG(budget) FROM Movies"
	cur.execute(sql)
	print("Average budget of all movies: ", cur.fetchone()[0])

def query2(cur):
	# show the movie titles and involved companies of movies produced in the US
	sql = """
		SELECT title, name
		FROM ProductionCompanies
		INNER JOIN (
			SELECT title, id_company
			FROM Movies
			INNER JOIN (
				SELECT id_movie, id_company
				FROM MoviesCompanies
				WHERE id_movie IN (
					SELECT id_movie
					FROM MoviesCountries
					WHERE id_country = 'US')) AS t0
			WHERE id = id_movie) AS t1
		WHERE id = id_company
	"""
	cur.execute(sql)
	results = cur.fetchall()
	pt = PrettyTable(['title', 'company'])
	for att in pt.align:
		pt.align[att] = "l"
	for row in results:
		pt.add_row(row)
	print(pt)
	#print(pt[0:5])

def query3(cur):
	# titles and revenues of the top 5 highest earning movies
	sql = """
		SELECT title, revenue
		FROM Movies
		ORDER BY revenue DESC
		LIMIT 5
	"""
	cur.execute(sql)
	results = cur.fetchall()
	pt = PrettyTable(['title', 'revenue'])
	for att in pt.align:
		pt.align[att] = "l"
	for row in results:
		pt.add_row(row)
	print(pt)

def query4(cur):
	# a movie's title and all its genres, provided it is of both Science Fiction and Mystery
	sql = """
			SELECT title, name
			FROM Genres
			INNER JOIN (
				SELECT title, id_genre
				FROM MoviesGenres
				INNER JOIN (
					SELECT title, id
					FROM Movies
					INNER JOIN (
						SELECT id_movie
						FROM MoviesGenres
						WHERE id_genre IN (
							SELECT id
							FROM Genres
							WHERE name = 'Science Fiction')
						INTERSECT 
						SELECT id_movie
						FROM MoviesGenres
						WHERE id_genre IN (
							SELECT id
							FROM Genres
							WHERE name = 'Mystery')) AS t0
					WHERE id = id_movie
				) AS t1
				WHERE id = id_movie
			) AS t2
			WHERE id = id_genre
	"""
	cur.execute(sql)
	results = cur.fetchall()
	pt = PrettyTable(['title', 'genre'])
	for att in pt.align:
		pt.align[att] = "l"
	for row in results:
		pt.add_row(row)
	print(pt)
	#print(pt[0:5])

def query5(cur):
	# a movie's title and its popularity, provided it's greater than the average popularity
	sql = """
		SELECT title, popularity
		FROM Movies
		WHERE popularity > (SELECT AVG(popularity) FROM Movies)
	"""
	cur.execute(sql)
	results = cur.fetchall()
	pt = PrettyTable(['title', 'popularity'])
	for att in pt.align:
		pt.align[att] = "l"
	for row in results:
		pt.add_row(row)
	print(pt)
	#print(pt[0:5])

def main():
	conn = pymysql.connect(host='localhost', port=3306, user='root', passwd=YOUR_PASSWORD_HERE)
	cur = conn.cursor()
	sql = "DROP DATABASE IF EXISTS HW5"
	cur.execute(sql)
	sql = "CREATE DATABASE HW5"
	cur.execute(sql)
	sql = "USE HW5"
	cur.execute(sql)
	conn.commit()
	createTables(conn, cur)

	if len(sys.argv) == 1:
		print('Error: No CSV file provided.')
		return
	else:
		readAndParse(conn, cur, sys.argv[1])

	for i in range(len(sys.argv)):
		if sys.argv[i] == '1':
			query1(cur)
		if sys.argv[i] == '2':
			query2(cur)
		if sys.argv[i] == '3':
			query3(cur)
		if sys.argv[i] == '4':
			query4(cur)
		if sys.argv[i] == '5':
			query5(cur)


if __name__ == '__main__':
	main()