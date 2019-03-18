# This image installs the -headless package of OpenJDK
# and so is missing many of the UI-related Java libraries
# and some common packages contained in the default tag
FROM openjdk:10.0.1-10-jre-slim as application
MAINTAINER Sateesh Pinnamaneni

ENV APP_BINARY=aepf-test-impl-all.jar

ENV TARGET_APP_BINARY=/usr/local/app/$APP_BINARY

COPY ./build/libs/$APP_BINARY $TARGET_APP_BINARY

HEALTHCHECK --interval=2s --timeout=2s --start-period=2s \
  CMD exit 0

CMD ["sh", "-c", "java -jar $TARGET_APP_BINARY"]