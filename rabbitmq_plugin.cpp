/**
 *  @file
 *  @copyright defined in eos/LICENSE.txt
 */
//
#include <stdlib.h>
#include <eosio/rabbitmq_plugin/rabbitmq_producer.hpp>
#include <eosio/rabbitmq_plugin/rabbitmq_plugin.hpp>

#include <eosio/chain/eosio_contract.hpp>
#include <eosio/chain/config.hpp>
#include <eosio/chain/exceptions.hpp>
#include <eosio/chain/transaction.hpp>
#include <eosio/chain/types.hpp>

#include <fc/io/json.hpp>
#include <fc/utf8.hpp>
#include <fc/variant.hpp>

#include <boost/chrono.hpp>
#include <boost/signals2/connection.hpp>
#include <boost/thread/thread.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/condition_variable.hpp>

// #include <queue>

namespace fc { class variant; }

namespace eosio {

    using chain::account_name;
    using chain::action_name;
    using chain::block_id_type;
    using chain::permission_name;
    using chain::transaction;
    using chain::signed_transaction;
    using chain::signed_block;
    using chain::transaction_id_type;
    using chain::packed_transaction;

static appbase::abstract_plugin& _rabbitmq_plugin = app().register_plugin<rabbitmq_plugin>();
using rabbitmq_producer_ptr = std::shared_ptr<class rabbitmq_producer>;

    class rabbitmq_plugin_impl {
    public:
        rabbitmq_plugin_impl();

        ~rabbitmq_plugin_impl();

        fc::optional<boost::signals2::scoped_connection> applied_transaction_connection;
        chain_plugin *chain_plug;
        struct trasaction_info_st {
            uint64_t block_number;
            fc::time_point block_time;
            chain::transaction_trace_ptr trace;
        };

        void applied_transaction(const chain::transaction_trace_ptr &);

        void process_applied_transaction(const trasaction_info_st &);

        void init();

        bool configured{false};

        uint32_t start_block_num = 0;

        boost::mutex mtx;
        boost::condition_variable condition;
        boost::thread consume_thread;
        boost::atomic<bool> done{false};
        boost::atomic<bool> startup{true};

        rabbitmq_producer_ptr producer;
        std::string m_applied_trx_exchange = "";
        std::string m_applied_trx_exchange_type = "";
        
    };

    void rabbitmq_plugin_impl::applied_transaction(const chain::transaction_trace_ptr &t) {
        try {
            auto &chain = chain_plug->chain();
            trasaction_info_st transactioninfo = trasaction_info_st{
                    .block_number = chain.pending_block_state()->block_num,
                    .block_time = chain.pending_block_time(),
                    .trace =chain::transaction_trace_ptr(t)
            };

            uint64_t time = (transactioninfo.block_time.time_since_epoch().count()/1000);
            string transaction_metadata_json =
                    "{\"block_number\":" + std::to_string(transactioninfo.block_number) + ",\"block_time\":" + std::to_string(time) +
                    ",\"trace\":" + fc::json::to_string(transactioninfo.trace).c_str() + "}";
            producer->trx_rabbitmq_sendmsg("", m_applied_trx_exchange,transaction_metadata_json);
            
        } catch (fc::exception &e) {
            elog("FC Exception while applied_transaction ${e}", ("e", e.to_string()));
            app.quit();
        } catch (std::exception &e) {
            elog("STD Exception while applied_transaction ${e}", ("e", e.what()));
            app.quit();
        } catch (...) {
            elog("Unknown exception while applied_transaction");
            app.quit();
        }
    }

    rabbitmq_plugin_impl::rabbitmq_plugin_impl()
    :producer(new rabbitmq_producer)
    {
    }

    rabbitmq_plugin_impl::~rabbitmq_plugin_impl() {
       if (!startup) {
          try {
             ilog( "rabbitmq_db_plugin shutdown in process please be patient this can take a few minutes" );
             done = true;
             condition.notify_one();

             consume_thread.join();
             producer->trx_rabbitmq_destroy();
          } catch( std::exception& e ) {
             elog( "Exception on rabbitmq_plugin shutdown of consume thread: ${e}", ("e", e.what()));
          }
       }
    }

    void rabbitmq_plugin_impl::init() {

        ilog("starting rabbitmq plugin thread");
        // consume_thread = boost::thread([this] { consume_blocks(); });
        startup = false;
    }

////////////
// rabbitmq_plugin
////////////

    rabbitmq_plugin::rabbitmq_plugin()
            : my(new rabbitmq_plugin_impl) {
    }

    rabbitmq_plugin::~rabbitmq_plugin() {
    }

    void rabbitmq_plugin::set_program_options(options_description &cli, options_description &cfg) {
        cfg.add_options()
                ("rabbitmq-applied-trx-exchange", bpo::value<std::string>()->default_value("trx.applied"),
                 "The exchange for appiled transaction.")
                 ("rabbitmq-applied-trx-exchange-type", bpo::value<std::string>()->default_value("fanout"),
                 "The exchange type for appiled transaction.")
                ("rabbitmq-username", bpo::value<std::string>()->default_value("guest"),
                 "the rabbitmq username (e.g. guest)")
                ("rabbitmq-password", bpo::value<std::string>()->default_value("guest"),
                 "the rabbitmq password (e.g. guest)")
                ("rabbitmq-hostname", bpo::value<std::string>()->default_value("127.0.0.1"),
                 "the rabbitmq hostname (e.g. localhost or 127.0.0.1)")
                ("rabbitmq-port", bpo::value<uint32_t>()->default_value(5672),
                 "the rabbitmq port (e.g. 5672)")                 
                 ;
    }

    void rabbitmq_plugin::plugin_initialize(const variables_map &options) {

        try {
            if (options.count("rabbitmq-hostname")) {
                auto hostname = options.at("rabbitmq-hostname").as<std::string>();
                auto username = options.at("rabbitmq-username").as<std::string>();
                auto password = options.at("rabbitmq-password").as<std::string>();
                uint32_t port = options.at("rabbitmq-port").as<uint32_t>();

                if (options.count("rabbitmq-applied-trx-exchange") != 0) {
                    my->m_applied_trx_exchange = options.at("rabbitmq-applied-trx-exchange").as<std::string>();
                }
                
                if (options.count("rabbitmq-applied-trx-exchange-type") != 0) {
                    my->m_applied_trx_exchange_type = options.at("rabbitmq-applied-trx-exchange-type").as<std::string>();
                }

                if (0!=my->producer->trx_rabbitmq_init(hostname, port, username, password)){
                    elog("trx_rabbitmq_init fail, killing node");
                    app.quit();
                } else{
                    elog("trx_rabbitmq_init ok");
                }

                if (0!=my->producer->trx_rabbitmq_assert_exchange(my->m_applied_trx_exchange, my->m_applied_trx_exchange_type)){
                    elog("trx_rabbitmq_init fail, killing node");
                    app.quit();
                } else{
                    elog("trx_rabbitmq_init ok");
                }
          
                ilog("initializing rabbitmq_plugin");
                my->configured = true;

                // hook up to signals on controller
                my->chain_plug = app().find_plugin<chain_plugin>();
                EOS_ASSERT(my->chain_plug, chain::missing_chain_plugin_exception, "");
                auto &chain = my->chain_plug->chain();
                
                my->applied_transaction_connection.emplace(
                        chain.applied_transaction.connect([&](const chain::transaction_trace_ptr &t) {
                            my->applied_transaction(t);
                        }));
                my->init();
            } else {
                wlog( "eosio::rabbitmq_plugin configured, but no --rabbitmq-hostname specified." );
                wlog( "rabbitmq_plugin disabled. killing eos node." );
                app.quit();
            }
        }
        FC_LOG_AND_RETHROW()
    }

    void rabbitmq_plugin::plugin_startup() {
    }

    void rabbitmq_plugin::plugin_shutdown() {
        my->applied_transaction_connection.reset();
        my.reset();
    }

} // namespace eosio


