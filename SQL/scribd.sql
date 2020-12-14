/*


Question : 


design db which can store sudio details given 
moviename, movie actor,movie director,movie date,actor age etc

A movie can have more than one actor or more than one director 
and actor and director can be same 


==========================

lets design the tables and normalise it by each entity type and link the movie details in one table

Tabl names 
Actor 
Movie
Director 

and movie_details - > this will connect the three entity table which contains details of movie id and its director id and actor _id

*/

/*
===================
following are table definitions 

*/
create database studio;

USE studio;

CREATE TABLE movie (
	movie_ID INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
	movie_name varchar(30)
	
	
);

CREATE TABLE actor (
	actor_ID INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
	actor_name varchar(30),
	actor_age INT
	
);

CREATE TABLE director (
	director_ID INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
	director_name varchar(30)
	
	
);

CREATE TABLE movie_details (
	movie_ID INT,
	actor_id INT,
    director_id INT,
FOREIGN KEY (movie_ID)
		REFERENCES movie(movie_ID)
        ON DELETE CASCADE,
        
FOREIGN KEY (actor_id)
		REFERENCES actor(actor_id)
        ON DELETE CASCADE,
FOREIGN KEY (director_id)
		REFERENCES director(director_id)
        ON DELETE CASCADE        
	
);


INSERT INTO movie 
	(movie_ID, movie_name) VALUES
		(001, 'DDLJ'),
		(002, 'WonderWomen'),
		(003, 'StarTrek')
		;

INSERT INTO movie 
	(movie_ID, movie_name) VALUES
		(004, 'KKKG');


INSERT INTO actor 
	(actor_ID, actor_name) VALUES
		(001, 'SK'),
		(002, 'RK'),
		(003, 'BK'),
        (004, 'PK')
		;
        
        

INSERT INTO director 
	(director_ID, director_name) VALUES
		(001, 'Ab'),
		(002, 'Cd'),
		(003, 'Ef'),
        (004, 'jk');
        
INSERT INTO director 
	(director_ID, director_name) VALUES			
        (005,'SK')
		;        

insert into movie_details
(movie_ID,actor_id,director_id) VALUES
(1,2,1),
(1,1,1),
(3,3,3),
(2,1,1),
(2,2,2),
(2,3,3)
;


insert into movie_details
(movie_ID,actor_id,director_id) VALUES
(4,1,5);


INSERT INTO movie 
	(movie_ID, movie_name) VALUES
		(005, 'DHG');


insert into movie_details
(movie_ID,actor_id,director_id) VALUES
(5,1,5);

/*
===============================
now lets query the db for answering following questions 
 
# get all the actors and directors from movie 'startrek'
# modify the query slightly to get the same details but actor name should start with 'S' 
 
*/

select D.director_ID,D.director_name,T.movie_ID,T.actor_id,T.director_id,T.actor_name from director D join
(select MD.movie_ID,MD.actor_id,MD.director_id,A.actor_name from movie_details MD 
join actor A 
where MD.movie_ID =
(select movie_ID from movie where movie_name='WonderWomen') and  A.actor_ID=MD.actor_ID )as T where T.director_id = D.director_ID and T.actor_name like 'S%';

/*
 NEXT, 
# find a movie where director and actor are same 
*/

select movie_name from movie where movie_ID in
(select movie_ID from movie_details MD join
(select D.director_ID,D.director_name,A.actor_name,A.actor_ID from director D
inner join actor A where A.actor_name = D.director_name) as tmp where tmp.director_ID = MD.director_ID and tmp.actor_ID=MD.actor_ID)


