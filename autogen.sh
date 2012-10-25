touch NEWS touch AUTHORS ChangeLog
if [ ! -d m4 ]; then
  mkdir m4
fi
aclocal
autoreconf -i
