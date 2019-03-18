# This image installs the -headless package of OpenJDK
# and so is missing many of the UI-related Java libraries
# and some common packages contained in the default tag
FROM openjdk:10.0.1-10-jre-slim as application
MAINTAINER Sateesh Pinnamaneni

# build args
ARG artifactVersion

# make build args available at runtime
ENV artifactVersion=${artifactVersion}

# standard source directories
ENV PROJECT_LIB_DIR=./build/libs
ENV PROJECT_DIST_DIR=./build/distributions

# standard target directories
ENV TARGET_APP_DIR=/usr/local/app
ENV TARGET_APP_LIB_DIR=${TARGET_APP_DIR}/lib

COPY ${PROJECT_DIST_DIR}/aepf-test-impl-${artifactVersion}.tar ${TARGET_APP_DIR}/
RUN tar -xvf ${TARGET_APP_DIR}/aepf-test-impl-${artifactVersion}.tar -C ${TARGET_APP_DIR} --strip=1
RUN rm -f ${TARGET_APP_DIR}/aepf-test-impl-${artifactVersion}.tar

HEALTHCHECK --interval=2s --timeout=2s --start-period=2s \
  CMD exit 0

CMD ["sh", "-c", "java -jar $TARGET_APP_LIB_DIR/aepf-test-impl-$artifactVersion.jar"]