#JHUSAK 20200802
#FROM node:8.9-alpine
#FROM node:12
FROM docker.io/library/centos:7

RUN yum install -y epel-release
RUN /usr/bin/curl --silent --location https://rpm.nodesource.com/setup_8.x | bash -
RUN yum install -y nodejs

#RUN yum install -y python2 node-gyp gcc make unixODBC

RUN mkdir -p /app
WORKDIR /app
COPY package.json /app/package.json
COPY . /app

RUN npm i  && ln -s /app/node_modules/ /node_modules

##RUN npm install -g nodemon
#RUN npm config set registry https://registry.npmjs.org

##JHUSAK 20200802
##RUN npm audit

##RUN npm install \
## && npm ls \
## && npm cache clean --force \
## && mv /app/node_modules /node_modules

##RUN npm install 
#RUN npm i npm@latest -g
##RUN npm ls
#RUN npm cache clean --force 
#RUN mv /app/node_modules /node_modules
#
##RUN chmod 755 /app/result_live_chk.sh

ENV PORT 8083
EXPOSE 8083

##CMD ["node", "server.js"]
CMD ["node", "src/index.js"]

##CMD exec /bin/sh -c "trap : TERM INT; (while true; do sleep 1000; done) & wait"
