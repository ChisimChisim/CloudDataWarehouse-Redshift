<h1>Could data warehouse project</h1>

<h2>1. purpose of this project</h2>

<p>The purpose of this project is building an ETL pipeline that extracts log and song data of a music streaming startup(Sparkify) from S3, staging them in Redshift, and transforming data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to. </p>

<h2>2. final database schema design</h2>
<h4>Fact Table</h4>
<ol>
      <li><strong>songplays</strong> - records in log data associated with song plays i.e. records with page NextSong
            <ul>
                  <li>songplay_id  --> PRIMARY KEY </li>
                  <li>start_time </li>
                  <li>user_id </li>
                  <li>level</li>
                  <li>user_id </li>
                  <li>song_id</li>
                  <li>artist_id</li>
                  <li>session_id</li>
                  <li>location</li>
                  <li>user_agent</li>
            </ul>
      </li>
</ol>

<h4>Dimension Tables</h4>
<ol>
      <li><strong>users</strong> - users in the app
            <ul>
                  <li>user_id  --> PRIMARY KEY</li>
                  <li>first_name</li>
                  <li>last_name</li>
                  <li>gender</li>
                  <li>level</li>
            </ul>
      </li>
      <li><strong>songs</strong> - songs in music database
            <ul>
                  <li>song_id  --> PRIMARY KEY</li>
                  <li>title</li>
                  <li>artist_id</li>
                  <li>year</li>
                  <li>duration</li>
            </ul>
      </li>
      <li><strong>artists</strong> - artists in music database
            <ul>
                  <li>artist_id  --> PRIMARY KEY</li>
                  <li>name</li>
                  <li>location</li>
                  <li>latitude</li>
                  <li>longitude</li>
            </ul>
      </li>
      <li><strong>time</strong> - timestamps of records in songplays broken down into specific units
            <ul>
                  <li>start_time  --> PRIMARY KEY</li>
                  <li>hour</li>
                  <li>day</li>
                  <li>week</li>
                  <li>month</li>
                  <li>year</li>
                  <li>weekday</li>
            </ul>
      </li>
</ol>

<h2>3. Explanation of the files</h2>
<p><strong>create_tables.py</strong> - creates staging tables from S3 and final 5 tables from staging tables using SQL queries in sql.queries.py</p>

<p><strong>etl.py</strong> - Load log and song data from S3 to staging tables and load them to final tables to analys. </p>

<p><strong>sql.queries.py</strong> - contains all sql queries.</p>

<p><strong>dwh.cfg</strong> - cinfig file including Redshift cluster, IAM role and S3 data info.</p>

<h2>4. How to run</h2>
<p>Run <strong><em>create_tables.py</em></strong> and then <strong><em>etl.py</em></strong></p>