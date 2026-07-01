FROM gradle:9.3.0-jdk25 AS build

COPY --chown=gradle:gradle . /usr/src/projectname

WORKDIR /usr/src/projectname

ARG GIT_HASH=unknown
ENV GIT_HASH=${GIT_HASH}

RUN gradle :app:installDist --no-daemon

FROM gcr.io/distroless/java25-debian13 AS runtime

COPY --from=build /usr/src/projectname/app/build/install/projectname /opt/projectname

ENTRYPOINT [ \
    "java", \
    "-cp", "/opt/projectname/lib/*", \
    "at.ac.uibk.dps.projectname.ProjectNameKt" \
]