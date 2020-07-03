#copy first 5 files from a folder to another
find . -maxdepth 1 -type f |head -5 |xargs -I {} mv /Users/kitu/2ndbatch/{} /Users/kitu/2ndbatch/first
