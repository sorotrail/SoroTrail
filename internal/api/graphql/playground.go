package graphql

// graphiqlHTML is the inlined GraphiQL playground page. Loaded from the
// graphql-playground HTML asset (the maintained successor to the older
// Apollo Playground) via CDN; no JS is bundled with this binary so the
// asset pull happens once per browser session. The dev-mode gate lives
// in api.Server where the route is registered.
const graphiqlHTML = `<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>SoroTrail GraphQL Playground</title>
  <link rel="stylesheet" href="https://unpkg.com/graphql-playground-react/build/static/css/index.css" />
</head>
<body>
  <div id="root">
    <noscript>You need to enable JavaScript to run the SoroTrail GraphQL Playground.</noscript>
  </div>
  <script src="https://unpkg.com/graphql-playground-react/build/static/js/middleware.js"></script>
  <script>
    window.addEventListener('load', function (event) {
      GraphQLPlayground.init(document.getElementById('root'), {
        endpoint: '/graphql',
        headers: { 'Accept': 'application/json' },
        settings: {
          'request.binaryChanges': false,
          'schema.polling.enable': false,
        },
      });
    });
  </script>
</body>
</html>
`
