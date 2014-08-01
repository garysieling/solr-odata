package org.apache.olingo.odata2.sample.service;

import java.sql.*;
import java.util.*;

import org.apache.olingo.odata2.api.edm.*;
import org.apache.olingo.odata2.api.edm.provider.Association;
import org.apache.olingo.odata2.api.edm.provider.AssociationEnd;
import org.apache.olingo.odata2.api.edm.provider.AssociationSet;
import org.apache.olingo.odata2.api.edm.provider.AssociationSetEnd;
import org.apache.olingo.odata2.api.edm.provider.ComplexProperty;
import org.apache.olingo.odata2.api.edm.provider.ComplexType;
import org.apache.olingo.odata2.api.edm.provider.CustomizableFeedMappings;
import org.apache.olingo.odata2.api.edm.provider.EdmProvider;
import org.apache.olingo.odata2.api.edm.provider.EntityContainer;
import org.apache.olingo.odata2.api.edm.provider.EntityContainerInfo;
import org.apache.olingo.odata2.api.edm.provider.EntitySet;
import org.apache.olingo.odata2.api.edm.provider.EntityType;
import org.apache.olingo.odata2.api.edm.provider.Facets;
import org.apache.olingo.odata2.api.edm.provider.FunctionImport;
import org.apache.olingo.odata2.api.edm.provider.Key;
import org.apache.olingo.odata2.api.edm.provider.NavigationProperty;
import org.apache.olingo.odata2.api.edm.provider.Property;
import org.apache.olingo.odata2.api.edm.provider.PropertyRef;
import org.apache.olingo.odata2.api.edm.provider.ReturnType;
import org.apache.olingo.odata2.api.edm.provider.Schema;
import org.apache.olingo.odata2.api.edm.provider.SimpleProperty;
import org.apache.olingo.odata2.api.exception.ODataException;
import org.apache.olingo.odata2.api.uri.KeyPredicate;

public class DB {
    public static String NAMESPACE = "tenant4";

    public static String tables =
            "select table_name \n" +
            "from information_schema.tables \n" +
            "where table_type  = 'BASE TABLE'\n" +
            "and table_schema = '" + NAMESPACE + "'\n" +
            ";";

    public static String columns =
            "SELECT *\n" +
                    "FROM information_schema.columns\n" +
                    "WHERE table_schema = '" + NAMESPACE + "'\n" +
                    "  AND table_name  = '%tablename%';\n";

    public static String keys =
            "SELECT\n" +
                    "    tc.constraint_name, tc.table_name, kcu.column_name, \n" +
                    "    ccu.table_name AS foreign_table_name,\n" +
                    "    ccu.column_name AS foreign_column_name \n" +
                    "FROM \n" +
                    "    information_schema.table_constraints AS tc \n" +
                    "    JOIN information_schema.key_column_usage AS kcu\n" +
                    "      ON tc.constraint_name = kcu.constraint_name\n" +
                    "    JOIN information_schema.constraint_column_usage AS ccu\n" +
                    "      ON ccu.constraint_name = tc.constraint_name\n" +
                    "WHERE constraint_type = 'FOREIGN KEY' AND tc.table_name='%tablename%';\n";

    public static String load =
            "SELECT *\n" +
            "FROM " + NAMESPACE + ".%tablename%\n";


    public static String TABLENAME = "%tablename%";
    private static Collection<? extends EntityType> _types;
    private static List<EntitySet> _sets;


    public static Connection getConnection() throws SQLException {
        try {
            Class.forName("org.postgresql.Driver");
        }
        catch (Exception e)
        {
            System.out.println(e);
        }
        String connStr = "jdbc:postgresql://gary-PC:5432/postgres";
        String user = "test";
        String password = "test";

        Properties props = new Properties();
        props.setProperty("user", user);
        props.setProperty("password", password);
        return DriverManager.getConnection(connStr, props);
    }


    public static Collection<? extends EntityType> getTypes() {
        if (_types != null)
            return _types;

        List<EntityType> types = new ArrayList<EntityType>();

        Statement stmt = null;
        try {
            Connection con = getConnection();
            stmt = con.createStatement();
            ResultSet rs = stmt.executeQuery(tables);
            while (rs.next()) {
                String name = rs.getString(1);

                stmt = con.createStatement();
                List<Property> properties = new ArrayList<Property>();

                ResultSet rsc = stmt.executeQuery(columns.replace(TABLENAME, name));
                while (rsc.next()) {
                    String colName = rsc.getString("column_name");
                    //Properties
                    properties.add(
                            new SimpleProperty()
                                    .setName(colName)
                                    .setType(EdmSimpleTypeKind.String)
                                    .setFacets(new Facets().setNullable(true)));
                   /* properties.add(new SimpleProperty().setName("Model").setType(EdmSimpleTypeKind.String).setFacets(new Facets().setNullable(false).setMaxLength(100).setDefaultValue("Hugo"))
                            .setCustomizableFeedMappings(new CustomizableFeedMappings().setFcTargetPath(EdmTargetPath.SYNDICATION_TITLE)));
                    properties.add(new SimpleProperty().setName("ManufacturerId").setType(EdmSimpleTypeKind.Int32));
                    properties.add(new SimpleProperty().setName("Price").setType(EdmSimpleTypeKind.Decimal));
                    properties.add(new SimpleProperty().setName("Currency").setType(EdmSimpleTypeKind.String).setFacets(new Facets().setMaxLength(3)));
                    properties.add(new SimpleProperty().setName("ModelYear").setType(EdmSimpleTypeKind.String).setFacets(new Facets().setMaxLength(4)));
                    properties.add(new SimpleProperty().setName("Updated").setType(EdmSimpleTypeKind.DateTime)
                            .setFacets(new Facets().setNullable(false).setConcurrencyMode(EdmConcurrencyMode.Fixed))
                            .setCustomizableFeedMappings(new CustomizableFeedMappings().setFcTargetPath(EdmTargetPath.SYNDICATION_UPDATED)));
                    properties.add(new SimpleProperty().setName("ImagePath").setType(EdmSimpleTypeKind.String));
                    */
                }

                rsc.close();

                //Navigation Properties
                List<NavigationProperty> navigationProperties = new ArrayList<NavigationProperty>();

             //   ResultSet rsk = stmt.executeQuery(keys.replace(TABLENAME, name));
            /*    while (rsk.next()) {
                    String constraintName = rsk.getString("constraint_name");
                    String colName = rsk.getString("column_name");
                    String foreignName = rsk.getString("foreign_table_name");
                    String foreignColName = rsk.getString("foreign_column_name");

                    navigationProperties.add(new NavigationProperty().setName(constraintName)
                            .setRelationship(
                                    new FullQualifiedName(NAMESPACE, name + "_" + foreignName + "_" + foreignName + "_" + name)
                            ).setFromRole(name).setToRole(foreignName));
                }

                rsk.close();*/

                //Key
                List<PropertyRef> keyProperties = new ArrayList<PropertyRef>();
                if (name.toLowerCase().endsWith("_r")) {
                    keyProperties.add(new PropertyRef().setName("i_id"));
                    keyProperties.add(new PropertyRef().setName("i_index"));
                } else {
                    keyProperties.add(new PropertyRef().setName("i_id"));
                }

                Key key = new Key().setKeys(keyProperties);

                types.add(
                        new EntityType().setName(name)
                                .setProperties(properties)
                                .setKey(key)
                                .setNavigationProperties(navigationProperties));
            }
        } catch (SQLException e ) {
            System.out.println(e.getMessage());
            // JDBCTutorialUtilities.printSQLException(e);
        } finally {
            try {
                if (stmt != null) {
                    stmt.close();
                }
            } catch (SQLException e) {
                System.out.println(e.getMessage());
            }
        }

        _types = types;

        return types;
    }


    public static List<Association> getAssociations() {
        return new ArrayList<Association>();
    }

    public static List<EntitySet> getEntitySets() {
        List<EntitySet> types = new ArrayList<EntitySet>();

        if (_sets != null) {
            return _sets;
        }

        Statement stmt = null;
        try {
            Connection con = getConnection();
            stmt = con.createStatement();
            ResultSet rs = stmt.executeQuery(tables);
            while (rs.next()) {
                String name = rs.getString(1);
                types.add(new EntitySet()
                        .setName(name)
                        .setEntityType(
                                new FullQualifiedName(NAMESPACE, name)
                        ));
            }
        } catch (SQLException e ) {
            System.out.println(e.getMessage());
            // JDBCTutorialUtilities.printSQLException(e);
        } finally {
            try {
                if (stmt != null) {
                    stmt.close();
                }
            } catch (SQLException e) {
                System.out.println(e.getMessage());
            }

        }
        _sets = types;

        return types;
    }

    public static List<AssociationSet> getAssociationSets() {

        List<AssociationSet> associationSets = new ArrayList<AssociationSet>();
        //associationSets.add(getAssociationSet(ENTITY_CONTAINER, ASSOCIATION_CAR_MANUFACTURER, ENTITY_SET_NAME_MANUFACTURERS, ROLE_1_2));

        return associationSets;
    }


    public static EntityType getEntity(String name) {
        Collection<? extends EntityType> types = getTypes();

        for (EntityType e : types) {
            if (e.getName().equals(name)) {
                return e;
            }
        }

        return null;
    }

    public static EntitySet getEntitySets(String name) {
        Collection<? extends EntitySet> types = getEntitySets();

        for (EntitySet e : types) {
            if (e.getName().equals(name)) {
                return e;
            }
        }

        return null;
    }

    public static List<Map<String,Object>> all(String type, KeyPredicate keyPredicate) {
        List<Map<String,Object>> results =
                new ArrayList<Map<String,Object>>();

        Statement stmt = null;
        try {
            Connection con = getConnection();

            String sql = load.replace(TABLENAME, type);

            // todo: sql injection
            // todo: types
            if (keyPredicate != null) {
                sql = sql + " WHERE " +
                        keyPredicate.getProperty().getName() +
                        " = '" +
                        // todo: escape for sql or use bind variables
                        // todo: if you include version # you can cache stuff
                        keyPredicate.getLiteral() + "'";
            }
            stmt = con.createStatement();
            ResultSet rsc = stmt.executeQuery(sql);
            while (rsc.next()) {
                // todo: some caching
                // todo: handle typing
                int width = rsc.getMetaData().getColumnCount();
                Map<String, Object> object =
                        new HashMap<String, Object>();

                for (int i = 1; i <= width; i++) {
                    object.put(
                            rsc.getMetaData().getColumnName(i),
                            rsc.getString(i)
                    );
                }

                results.add(object);
            }

            rsc.close();

        } catch (SQLException e ) {
            System.out.println(e.getMessage());
            // JDBCTutorialUtilities.printSQLException(e);
        } catch (EdmException e) {
            e.printStackTrace();
        } finally {
            try {
                if (stmt != null) {
                    stmt.close();
                }
            } catch (SQLException e) {
                System.out.println(e.getMessage());
            }
        }

        return results;
    }

    public static Map<String,Object> id(final String type, final String id) {
        Map<String,Object> results =
                new HashMap<String,Object>();

        Statement stmt = null;
        try {
            Connection con = getConnection();

            String sql = load.replace(TABLENAME, type);

            // todo: sql injection
            // todo: types

                sql = sql + " WHERE " +
                        "i_id" +
                        " = '" +
                        // todo: escape for sql or use bind variables
                        // todo: if you include version # you can cache stuff
                        id + "'";
            stmt = con.createStatement();
            ResultSet rsc = stmt.executeQuery(sql);
            while (rsc.next()) {
                // todo: some caching
                // todo: handle typing
                int width = rsc.getMetaData().getColumnCount();
                for (int i = 1; i <= width; i++) {
                    String column = rsc.getString(i);
                    results.put(
                            rsc.getMetaData().getColumnName(i),
                            column
                    );
                }
            }

            rsc.close();

        } catch (SQLException e ) {
            System.out.println(e.getMessage());
            // JDBCTutorialUtilities.printSQLException(e);
        } finally {
            try {
                if (stmt != null) {
                    stmt.close();
                }
            } catch (SQLException e) {
                System.out.println(e.getMessage());
            }
        }

        return results;
    }

}