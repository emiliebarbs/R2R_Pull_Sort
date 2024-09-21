# R2R Ocean Acoustics Data Puller

Hi there!

This is an automated script to facilitate data package pulldown from the 
Lamont-Doherty Earth Observatory servers that provide Ocean Acoustic and Bathmetry data to NCEI.

## How It Works

The script connects to the SFTP server, gathers metadata which is used to query the
R2R API, which in turn populates a SQLite file that serves as an inventory. From there
the users are prompted to choose which data they want to pull down.

The next step takes the tarballs that land on NCEI's servers, validates and decompresses
them, and finally untars them and moves them to the proper landing directory based on
what NCEI data type it contains. 

Feel free to check it out!


#### NOTE: This project has been modifed to conceal the sensitve information used to access server and landing spaces. 
