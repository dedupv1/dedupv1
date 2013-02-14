set -e

if [ ! -d "/opt/dedupv1" ]; then
	echo "Create user-writable directory /opt/dedupv1"
	exit 1
fi
if [ `uname` = 'Darwin' ]; then
	echo "use install-libs-mac.sh on Darwin systems"
	exit 1
fi


if [ "$#" -eq 0 ]
then 
	if [ ! "x" = "x$ONLY_SCST" ]; then
		python ./install-libs.py scst $@
	else
    	python ./install-libs.py all
    fi
else
	python ./install-libs.py $@
fi
echo "Finished thirdparty install"
