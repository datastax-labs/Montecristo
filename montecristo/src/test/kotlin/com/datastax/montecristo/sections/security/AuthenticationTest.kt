/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datastax.montecristo.sections.security

import com.datastax.montecristo.logs.Searcher
import com.datastax.montecristo.model.Cluster
import com.datastax.montecristo.model.ConfigSource
import com.datastax.montecristo.model.application.ConfigValue
import com.datastax.montecristo.model.application.ConfigurationSetting
import com.datastax.montecristo.model.profiles.ExecutionProfile
import com.datastax.montecristo.model.versions.DatabaseVersion
import com.datastax.montecristo.sections.structure.Recommendation
import io.mockk.every
import io.mockk.mockk
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test

internal class AuthenticationTest {

    @Test
    fun getCassDocumentConsistentHasAuth() {

        val authenticatorConfigSetting = ConfigurationSetting("authenticator", mapOf(Pair("node1", ConfigValue(true, "AllowAllAuthenticator","PasswordAuthenticator"))))
        val authorizerConfigSetting = ConfigurationSetting("authorizer", mapOf(Pair("node1", ConfigValue(true, "AllowAllAuthorizer","PasswordAuthenticator"))))

        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.getSetting("authenticator", ConfigSource.CASS, "AllowAllAuthenticator") } returns authenticatorConfigSetting
        every { cluster.getSetting("authorizer", ConfigSource.CASS, "AllowAllAuthorizer") } returns authorizerConfigSetting
        every { cluster.isDse } returns false
        every { cluster.databaseVersion } returns DatabaseVersion.latest311()
        every { cluster.getSetting("permissions_validity_in_ms", ConfigSource.CASS, "2000") } returns ConfigurationSetting("permissions_validity_in_ms", mapOf(Pair("node1", ConfigValue(true, "2000","30000"))))
        every { cluster.getSetting("permissions_update_interval_in_ms", ConfigSource.CASS, "2000") } returns ConfigurationSetting("permissions_update_interval_in_ms",  mapOf(Pair("node1", ConfigValue(true, "2000","30000"))))
        every { cluster.getSetting("roles_validity_in_ms", ConfigSource.CASS, "2000") } returns  ConfigurationSetting("roles_validity_in_ms",  mapOf(Pair("node1", ConfigValue(true, "2000","30000"))))
        every { cluster.getSetting("roles_update_interval_in_ms", ConfigSource.CASS, "2000") } returns ConfigurationSetting("roles_update_interval_in_ms",  mapOf(Pair("node1", ConfigValue(true, "2000","30000"))))
        every { cluster.getSetting("credentials_validity_in_ms", ConfigSource.CASS, "2000") } returns  ConfigurationSetting("credentials_validity_in_ms",  mapOf(Pair("node1", ConfigValue(true, "2000","30000"))))
        every { cluster.getSetting("credentials_update_interval_in_ms", ConfigSource.CASS, "2000") } returns ConfigurationSetting("credentials_update_interval_in_ms",  mapOf(Pair("node1", ConfigValue(true, "2000","30000"))))

        val auth = Authentication()
        val recs: MutableList<Recommendation> = mutableListOf()
        val searcher = mockk<Searcher>(relaxed = true)
        val template = auth.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(0)
        assertThat(template).contains("Authentication is currently enabled")
        assertThat(template).contains("authenticator: PasswordAuthenticator")
        assertThat(template).contains("Authorization is currently enabled")
        assertThat(template).contains("authorizer: PasswordAuthenticator")
        assertThat(template).doesNotContain("Enabling authentication and authorization will require additional changes including")
    }

    @Test
    fun getCassDocumentConsistentHasNoAuth() {

        val authenticatorConfigSetting = ConfigurationSetting("authenticator", mapOf(Pair("node1", ConfigValue(false, "AllowAllAuthenticator", ""))))
        val authorizerConfigSetting = ConfigurationSetting("authorizer", mapOf(Pair("node1",  ConfigValue(false, "AllowAllAuthorizer",""))))

        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.isDse } returns false
        every { cluster.databaseVersion } returns DatabaseVersion.latest311()
        every { cluster.getSetting("authenticator", ConfigSource.CASS, "AllowAllAuthenticator") } returns authenticatorConfigSetting
        every { cluster.getSetting("authorizer", ConfigSource.CASS, "AllowAllAuthorizer") } returns authorizerConfigSetting
        every { cluster.getSetting("permissions_validity_in_ms", ConfigSource.CASS, "2000") } returns ConfigurationSetting("permissions_validity_in_ms", mapOf(Pair("node1", ConfigValue(true, "2000","30000"))))
        every { cluster.getSetting("permissions_update_interval_in_ms", ConfigSource.CASS, "2000") } returns ConfigurationSetting("permissions_update_interval_in_ms",  mapOf(Pair("node1",ConfigValue(true, "2000", "30000"))))
        every { cluster.getSetting("roles_validity_in_ms", ConfigSource.CASS, "2000") } returns  ConfigurationSetting("roles_validity_in_ms",  mapOf(Pair("node1", ConfigValue(true, "2000","30000"))))
        every { cluster.getSetting("roles_update_interval_in_ms", ConfigSource.CASS, "2000") } returns ConfigurationSetting("roles_update_interval_in_ms",  mapOf(Pair("node1", ConfigValue(true, "2000","30000"))))
        every { cluster.getSetting("credentials_validity_in_ms", ConfigSource.CASS, "2000") } returns  ConfigurationSetting("credentials_validity_in_ms",  mapOf(Pair("node1", ConfigValue(true, "2000","30000"))))
        every { cluster.getSetting("credentials_update_interval_in_ms", ConfigSource.CASS, "2000") } returns ConfigurationSetting("credentials_update_interval_in_ms",  mapOf(Pair("node1", ConfigValue(true, "2000","30000"))))

        val auth = Authentication()
        val recs: MutableList<Recommendation> = mutableListOf()
        val searcher = mockk<Searcher>(relaxed = true)
        val template = auth.getDocument(cluster, searcher, recs,ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(2)
        assertThat(recs[0].longForm).isEqualTo(auth.authenticatorRecommendation)
        assertThat(recs[1].longForm).isEqualTo(auth.authorizerRecommendation)
        assertThat(template).contains("Authentication is currently disabled")
        assertThat(template).contains("authenticator: AllowAllAuthenticator")
        assertThat(template).contains("Authorization is currently disabled")
        assertThat(template).contains("authorizer: AllowAllAuthorizer")
        assertThat(template).contains("Enabling authentication and authorization will require additional changes including")
    }

    @Test
    fun getCassDocumentInconsistent() {

        val authenticatorConfigSetting = ConfigurationSetting("authenticator", mapOf(Pair("node1",  ConfigValue(true, "AllowAllAuthenticator","PasswordAuthenticator")), Pair("node2", ConfigValue(true, "AllowAllAuthenticator","AllowAllAuthenticator"))))
        val authorizerConfigSetting = ConfigurationSetting("authorizer", mapOf(Pair("node1", ConfigValue(true, "AllowAllAuthorizer","CassandraAuthorizer")),Pair("node2",ConfigValue(true, "AllowAllAuthorizer","AllowAllAuthorizer"))))

        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.isDse } returns false
        every { cluster.databaseVersion } returns DatabaseVersion.latest311()
        every { cluster.getSetting("authenticator", ConfigSource.CASS, "AllowAllAuthenticator") } returns authenticatorConfigSetting
        every { cluster.getSetting("authorizer", ConfigSource.CASS, "AllowAllAuthorizer") } returns authorizerConfigSetting
        every { cluster.getSetting("permissions_validity_in_ms", ConfigSource.CASS, "2000") } returns ConfigurationSetting("permissions_validity_in_ms", mapOf(Pair("node1", ConfigValue(true, "2000","30000"))))
        every { cluster.getSetting("permissions_update_interval_in_ms", ConfigSource.CASS, "2000") } returns ConfigurationSetting("permissions_update_interval_in_ms",  mapOf(Pair("node1", ConfigValue(true, "2000","30000"))))
        every { cluster.getSetting("roles_validity_in_ms", ConfigSource.CASS, "2000") } returns  ConfigurationSetting("roles_validity_in_ms",  mapOf(Pair("node1", ConfigValue(true, "2000","30000"))))
        every { cluster.getSetting("roles_update_interval_in_ms", ConfigSource.CASS, "2000") } returns ConfigurationSetting("roles_update_interval_in_ms",  mapOf(Pair("node1", ConfigValue(true, "2000","30000"))))
        every { cluster.getSetting("credentials_validity_in_ms", ConfigSource.CASS, "2000") } returns  ConfigurationSetting("credentials_validity_in_ms",  mapOf(Pair("node1", ConfigValue(true, "2000","30000"))))
        every { cluster.getSetting("credentials_update_interval_in_ms", ConfigSource.CASS, "2000") } returns ConfigurationSetting("credentials_update_interval_in_ms",  mapOf(Pair("node1", ConfigValue(true, "2000","30000"))))

        val auth = Authentication()
        val recs: MutableList<Recommendation> = mutableListOf()
        val searcher = mockk<Searcher>(relaxed = true)
        val template = auth.getDocument(cluster, searcher, recs,ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(2)
        assertThat(recs[0].longForm).isEqualTo(auth.authenticatorRecommendation)
        assertThat(recs[1].longForm).isEqualTo(auth.authorizerRecommendation)
        assertThat(template).contains("Enabling authentication and authorization will require additional changes including")
    }

    @Test
    fun getDseDocumentConsistentHasAuth() {

        val authenticatorConfigSetting = ConfigurationSetting("authenticator", mapOf(Pair("node1", ConfigValue(true, "AllowAllAuthenticator","com.datastax.bdp.cassandra.auth.DseAuthenticator"))))
        val authorizerConfigSetting = ConfigurationSetting("authorizer", mapOf(Pair("node1", ConfigValue(true, "AllowAllAuthorizer","com.datastax.bdp.cassandra.auth.DseAuthorizer"))))
        val dseAuthenticatorConfigSetting = ConfigurationSetting("enabled", mapOf(Pair("node1",  ConfigValue(true, "false","true"))))
        val dseAuthorizerConfigSetting = ConfigurationSetting("enabled", mapOf(Pair("node1",  ConfigValue(true, "false","true"))))

        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.isDse } returns true
        every { cluster.databaseVersion } returns DatabaseVersion.fromString("6.7.10", true)
        every { cluster.getSetting("authenticator", ConfigSource.CASS, "AllowAllAuthenticator") } returns authenticatorConfigSetting
        every { cluster.getSetting("authorizer", ConfigSource.CASS, "AllowAllAuthorizer") } returns authorizerConfigSetting
        every { cluster.getSetting("authentication_options.enabled", ConfigSource.DSE, "false") } returns dseAuthenticatorConfigSetting
        every { cluster.getSetting("authorization_options.enabled", ConfigSource.DSE, "false") } returns dseAuthorizerConfigSetting
        every { cluster.getSetting("permissions_validity_in_ms", ConfigSource.CASS, "120000") } returns ConfigurationSetting("permissions_validity_in_ms", mapOf(Pair("node1", ConfigValue(true, "12000","30000"))))
        every { cluster.getSetting("permissions_update_interval_in_ms", ConfigSource.CASS, "120000") } returns ConfigurationSetting("permissions_update_interval_in_ms",  mapOf(Pair("node1", ConfigValue(true, "12000","30000"))))
        every { cluster.getSetting("roles_validity_in_ms", ConfigSource.CASS, "120000") } returns  ConfigurationSetting("roles_validity_in_ms",  mapOf(Pair("node1", ConfigValue(true, "12000","30000"))))
        every { cluster.getSetting("roles_update_interval_in_ms", ConfigSource.CASS, "120000") } returns ConfigurationSetting("roles_update_interval_in_ms",  mapOf(Pair("node1", ConfigValue(true, "12000","30000"))))
        every { cluster.getSetting("credentials_validity_in_ms", ConfigSource.CASS, "120000") } returns  ConfigurationSetting("credentials_validity_in_ms",  mapOf(Pair("node1", ConfigValue(true, "12000","30000"))))
        every { cluster.getSetting("credentials_update_interval_in_ms", ConfigSource.CASS, "120000") } returns ConfigurationSetting("credentials_update_interval_in_ms",  mapOf(Pair("node1", ConfigValue(true, "12000","30000"))))

        val auth = Authentication()
        val recs: MutableList<Recommendation> = mutableListOf()
        val searcher = mockk<Searcher>(relaxed = true)
        val template = auth.getDocument(cluster, searcher, recs,ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(0)
        assertThat(template).contains("Authentication is currently enabled")
        assertThat(template).contains("authenticator: com.datastax.bdp.cassandra.auth.DseAuthenticator")
        assertThat(template).contains("Authorization is currently enabled")
        assertThat(template).contains("authorizer: com.datastax.bdp.cassandra.auth.DseAuthorizer")
        assertThat(template).doesNotContain("Enabling authentication and authorization will require additional changes including")
    }

    @Test
    fun getDseDocumentConsistentHasNoAuth() {

        val authenticatorConfigSetting = ConfigurationSetting("authenticator", mapOf(Pair("node1", ConfigValue(true, "AllowAllAuthenticator","com.datastax.bdp.cassandra.auth.DseAuthenticator"))))
        val authorizerConfigSetting = ConfigurationSetting("authorizer", mapOf(Pair("node1", ConfigValue(true, "AllowAllAuthorizer","com.datastax.bdp.cassandra.auth.DseAuthorizer"))))
        val dseAuthenticatorConfigSetting = ConfigurationSetting("enabled", mapOf(Pair("node1", ConfigValue(false, "false",""))))
        val dseAuthorizerConfigSetting = ConfigurationSetting("enabled", mapOf(Pair("node1", ConfigValue(false, "false", ""))))

        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.isDse } returns true
        every { cluster.databaseVersion } returns DatabaseVersion.fromString("6.7.10", true)
        every { cluster.getSetting("authenticator", ConfigSource.CASS, "AllowAllAuthenticator") } returns authenticatorConfigSetting
        every { cluster.getSetting("authorizer", ConfigSource.CASS, "AllowAllAuthorizer") } returns authorizerConfigSetting
        every { cluster.getSetting("authentication_options.enabled", ConfigSource.DSE, "false") } returns dseAuthenticatorConfigSetting
        every { cluster.getSetting("authorization_options.enabled", ConfigSource.DSE, "false") } returns dseAuthorizerConfigSetting
        every { cluster.getSetting("permissions_validity_in_ms", ConfigSource.CASS, "120000") } returns ConfigurationSetting("permissions_validity_in_ms", mapOf(Pair("node1", ConfigValue(true, "12000","30000"))))
        every { cluster.getSetting("permissions_update_interval_in_ms", ConfigSource.CASS, "120000") } returns ConfigurationSetting("permissions_update_interval_in_ms",  mapOf(Pair("node1", ConfigValue(true, "12000","30000"))))
        every { cluster.getSetting("roles_validity_in_ms", ConfigSource.CASS, "120000") } returns  ConfigurationSetting("roles_validity_in_ms",  mapOf(Pair("node1", ConfigValue(true, "12000","30000"))))
        every { cluster.getSetting("roles_update_interval_in_ms", ConfigSource.CASS, "120000") } returns ConfigurationSetting("roles_update_interval_in_ms",  mapOf(Pair("node1", ConfigValue(true, "12000","30000"))))
        every { cluster.getSetting("credentials_validity_in_ms", ConfigSource.CASS, "120000") } returns  ConfigurationSetting("credentials_validity_in_ms",  mapOf(Pair("node1", ConfigValue(true, "12000","30000"))))
        every { cluster.getSetting("credentials_update_interval_in_ms", ConfigSource.CASS, "120000") } returns ConfigurationSetting("credentials_update_interval_in_ms",  mapOf(Pair("node1", ConfigValue(true, "12000","30000"))))

        val auth = Authentication()
        val recs: MutableList<Recommendation> = mutableListOf()
        val searcher = mockk<Searcher>(relaxed = true)
        val template = auth.getDocument(cluster, searcher, recs,ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(2)
        assertThat(recs[0].longForm).isEqualTo(auth.dseAuthenticatorRecommendation)
        assertThat(recs[1].longForm).isEqualTo(auth.dseAuthorizerRecommendation)
        assertThat(template).contains("Authentication is currently using the DSE Authenticator, but DSE authentication is disabled, resulting in no authentication")
        assertThat(template).contains("authenticator: com.datastax.bdp.cassandra.auth.DseAuthenticator")
        assertThat(template).contains("Authorization is currently using the DSE Authorizer, but DSE authorization is disabled, resulting in no authorization")
        assertThat(template).contains("authorizer: com.datastax.bdp.cassandra.auth.DseAuthorizer")
        assertThat(template).contains("Enabling authentication and authorization will require additional changes including")
    }

    @Test
    fun getDseDocumentInconsistent() {

        val authenticatorConfigSetting = ConfigurationSetting("authenticator", mapOf(Pair("node1", ConfigValue(true, "AllowAllAuthenticator","com.datastax.bdp.cassandra.auth.DseAuthenticator")), Pair("node2", ConfigValue(true, "AllowAllAuthenticator","AllowAllAuthenticator"))))
        val authorizerConfigSetting = ConfigurationSetting("authorizer", mapOf(Pair("node1", ConfigValue(true, "AllowAllAuthorizer","CassandraAuthorizer")), Pair("node2", ConfigValue(true, "AllowAllAuthorizer","AllowAllAuthorizer"))))
        val dseAuthenticatorConfigSetting = ConfigurationSetting("enabled", mapOf(Pair("node1", ConfigValue(false, "false",""))))
        val dseAuthorizerConfigSetting = ConfigurationSetting("enabled", mapOf(Pair("node1", ConfigValue(false, "false", ""))))

        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.isDse } returns true
        every { cluster.databaseVersion } returns DatabaseVersion.fromString("6.7.10", true)
        every { cluster.getSetting("authenticator", ConfigSource.CASS, "AllowAllAuthenticator") } returns authenticatorConfigSetting
        every { cluster.getSetting("authorizer", ConfigSource.CASS, "AllowAllAuthorizer") } returns authorizerConfigSetting
        every { cluster.getSetting("authentication_options.enabled", ConfigSource.DSE, "false") } returns dseAuthenticatorConfigSetting
        every { cluster.getSetting("authorization_options.enabled", ConfigSource.DSE, "false") } returns dseAuthorizerConfigSetting
        every { cluster.getSetting("permissions_validity_in_ms", ConfigSource.CASS, "120000") } returns ConfigurationSetting("permissions_validity_in_ms", mapOf(Pair("node1", ConfigValue(true, "12000","30000"))))
        every { cluster.getSetting("permissions_update_interval_in_ms", ConfigSource.CASS, "120000") } returns ConfigurationSetting("permissions_update_interval_in_ms",  mapOf(Pair("node1", ConfigValue(true, "12000","30000"))))
        every { cluster.getSetting("roles_validity_in_ms", ConfigSource.CASS, "120000") } returns  ConfigurationSetting("roles_validity_in_ms",  mapOf(Pair("node1",ConfigValue(true, "12000", "30000"))))
        every { cluster.getSetting("roles_update_interval_in_ms", ConfigSource.CASS, "120000") } returns ConfigurationSetting("roles_update_interval_in_ms",  mapOf(Pair("node1", ConfigValue(true, "12000","30000"))))
        every { cluster.getSetting("credentials_validity_in_ms", ConfigSource.CASS, "120000") } returns  ConfigurationSetting("credentials_validity_in_ms",  mapOf(Pair("node1", ConfigValue(true, "12000","30000"))))
        every { cluster.getSetting("credentials_update_interval_in_ms", ConfigSource.CASS, "120000") } returns ConfigurationSetting("credentials_update_interval_in_ms",  mapOf(Pair("node1", ConfigValue(true, "12000","30000"))))

        val auth = Authentication()
        val recs: MutableList<Recommendation> = mutableListOf()
        val searcher = mockk<Searcher>(relaxed = true)
        val template = auth.getDocument(cluster, searcher, recs,ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(2)
        assertThat(recs[0].longForm).isEqualTo(auth.dseAuthenticatorRecommendation)
        assertThat(recs[1].longForm).isEqualTo(auth.dseAuthorizerRecommendation)
        assertThat(template).contains("Authentication settings are inconsistent across the cluster")
        assertThat(template).contains("node1 = com.datastax.bdp.cassandra.auth.DseAuthenticator")
        assertThat(template).contains("node2 = AllowAllAuthenticator")
        assertThat(template).contains("Authorization settings are inconsistent across the cluster")
        assertThat(template).contains("node1 = CassandraAuthorizer")
        assertThat(template).contains("node2 = AllowAllAuthorizer")
        assertThat(template).contains("Enabling authentication and authorization will require additional changes including")
    }

    @Test
    fun getDocumentAuthTooLow() {

        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.isDse } returns true
        every { cluster.databaseVersion } returns DatabaseVersion.fromString("5.1.20", true)
        every { cluster.getSetting("permissions_validity_in_ms", ConfigSource.CASS, "2000") } returns ConfigurationSetting("permissions_validity_in_ms", mapOf(Pair("node1", ConfigValue(true, "2000","2000"))))
        every { cluster.getSetting("permissions_update_interval_in_ms", ConfigSource.CASS, "2000") } returns ConfigurationSetting("permissions_update_interval_in_ms",  mapOf(Pair("node1",  ConfigValue(true, "2000","2000"))))
        every { cluster.getSetting("roles_validity_in_ms", ConfigSource.CASS, "2000") } returns  ConfigurationSetting("roles_validity_in_ms",  mapOf(Pair("node1", ConfigValue(true, "2000", "2000"))))
        every { cluster.getSetting("roles_update_interval_in_ms", ConfigSource.CASS, "2000") } returns ConfigurationSetting("roles_update_interval_in_ms",  mapOf(Pair("node1",  ConfigValue(true, "2000","2000"))))
        every { cluster.getSetting("credentials_validity_in_ms", ConfigSource.CASS, "2000") } returns  ConfigurationSetting("credentials_validity_in_ms",  mapOf(Pair("node1",  ConfigValue(true, "2000","2000"))))
        every { cluster.getSetting("credentials_update_interval_in_ms", ConfigSource.CASS, "2000") } returns ConfigurationSetting("credentials_update_interval_in_ms",  mapOf(Pair("node1",  ConfigValue(true, "2000","2000"))))

        val auth = Authentication()
        val recs: MutableList<Recommendation> = mutableListOf()
        auth.checkAuthSettings(cluster, recs)
        assertThat(recs.size).isEqualTo(1)
        assertThat(recs[0].longForm).contains("permissions_validity_in_ms")
        assertThat(recs[0].longForm).contains("permissions_update_interval_in_ms")
        assertThat(recs[0].longForm).contains("roles_validity_in_ms")
        assertThat(recs[0].longForm).contains("roles_update_interval_in_ms")
        assertThat(recs[0].longForm).contains("credentials_validity_in_ms")
        assertThat(recs[0].longForm).contains("credentials_update_interval_in_ms")
    }

    @Test
    fun getDocumentAuthTooLowDSE6() {
        val authenticatorConfigSetting = ConfigurationSetting("authenticator", mapOf(Pair("node1", ConfigValue(true, "AllowAllAuthenticator","com.datastax.bdp.cassandra.auth.DseAuthenticator")), Pair("node2", ConfigValue(true, "AllowAllAuthenticator","AllowAllAuthenticator"))))
        val authorizerConfigSetting = ConfigurationSetting("authorizer", mapOf(Pair("node1", ConfigValue(true, "AllowAllAuthorizer","CassandraAuthorizer")), Pair("node2", ConfigValue(true, "AllowAllAuthorizer","AllowAllAuthorizer"))))

        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.isDse } returns true
        every { cluster.databaseVersion } returns DatabaseVersion.fromString("6.7.10", true)

        val dseAuthenticatorConfigSetting = ConfigurationSetting("enabled", mapOf(Pair("node1",  ConfigValue(true, "true","true"))))
        val dseAuthorizerConfigSetting = ConfigurationSetting("enabled", mapOf(Pair("node1",  ConfigValue(true, "true","true"))))
        every { cluster.getSetting("authenticator", ConfigSource.CASS, "AllowAllAuthenticator") } returns authenticatorConfigSetting
        every { cluster.getSetting("authorizer", ConfigSource.CASS, "AllowAllAuthorizer") } returns authorizerConfigSetting
        every { cluster.getSetting("authentication_options.enabled", ConfigSource.DSE, "false") } returns dseAuthenticatorConfigSetting
        every { cluster.getSetting("authorization_options.enabled", ConfigSource.DSE, "false") } returns dseAuthorizerConfigSetting
        every { cluster.getSetting("permissions_validity_in_ms", ConfigSource.CASS, "120000") } returns ConfigurationSetting("permissions_validity_in_ms", mapOf(Pair("node1",  ConfigValue(true, "12000","12000"))))
        every { cluster.getSetting("permissions_update_interval_in_ms", ConfigSource.CASS, "120000") } returns ConfigurationSetting("permissions_update_interval_in_ms",  mapOf(Pair("node1",  ConfigValue(true, "12000","12000"))))
        every { cluster.getSetting("roles_validity_in_ms", ConfigSource.CASS, "120000") } returns  ConfigurationSetting("roles_validity_in_ms",  mapOf(Pair("node1",  ConfigValue(true, "12000","12000"))))
        every { cluster.getSetting("roles_update_interval_in_ms", ConfigSource.CASS, "120000") } returns ConfigurationSetting("roles_update_interval_in_ms",  mapOf(Pair("node1",  ConfigValue(true, "12000","12000"))))

        val auth = Authentication()
        val recs: MutableList<Recommendation> = mutableListOf()
        auth.checkAuthSettings(cluster, recs)
        assertThat(recs.size).isEqualTo(1)
        assertThat(recs[0].longForm).contains("permissions_validity_in_ms")
        assertThat(recs[0].longForm).contains("permissions_update_interval_in_ms")
        assertThat(recs[0].longForm).contains("roles_validity_in_ms")
        assertThat(recs[0].longForm).contains("roles_update_interval_in_ms")
        assertThat(recs[0].longForm).doesNotContain("credentials_validity_in_ms")
        assertThat(recs[0].longForm).doesNotContain("credentials_update_interval_in_ms")
    }

    @Test
    fun getDocumentAuthInconsistent() {
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.isDse } returns true
        every { cluster.databaseVersion } returns DatabaseVersion.fromString("6.8.10", true)
        every { cluster.getSetting("permissions_validity_in_ms", ConfigSource.CASS, "120000") } returns ConfigurationSetting("permissions_validity_in_ms", mapOf(Pair("node1", ConfigValue(true, "12000", "120000")), Pair("node2", ConfigValue(true, "12000","125000"))))
        every { cluster.getSetting("permissions_update_interval_in_ms", ConfigSource.CASS, "120000") } returns ConfigurationSetting("permissions_update_interval_in_ms",  mapOf(Pair("node1",  ConfigValue(true, "12000","120000")), Pair("node2", ConfigValue(true, "12000","125000"))))
        every { cluster.getSetting("roles_validity_in_ms", ConfigSource.CASS, "120000") } returns  ConfigurationSetting("roles_validity_in_ms",  mapOf(Pair("node1",  ConfigValue(true, "12000","120000")), Pair("node2", ConfigValue(true, "12000","125000"))))
        every { cluster.getSetting("roles_update_interval_in_ms", ConfigSource.CASS, "120000") } returns ConfigurationSetting("roles_update_interval_in_ms",  mapOf(Pair("node1",  ConfigValue(true, "12000","120000")), Pair("node2", ConfigValue(true, "12000","125000"))))

        val auth = Authentication()
        val recs: MutableList<Recommendation> = mutableListOf()
        auth.checkAuthSettings(cluster, recs)
        assertThat(recs.size).isEqualTo(1)
        assertThat(recs[0].longForm).contains("We recommend that the authentication settings on the nodes are made consistent")
        assertThat(recs[0].longForm).contains("permissions_validity_in_ms")
        assertThat(recs[0].longForm).contains("permissions_update_interval_in_ms")
        assertThat(recs[0].longForm).contains("roles_validity_in_ms")
        assertThat(recs[0].longForm).contains("roles_update_interval_in_ms")
        assertThat(recs[0].longForm).doesNotContain("credentials_validity_in_ms")
        assertThat(recs[0].longForm).doesNotContain("credentials_update_interval_in_ms")
    }
}