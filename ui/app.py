import streamlit as st
import os
import sys
import json
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv(override=True)

from neo4j import GraphDatabase

st.set_page_config(
    page_title="Systematic Problem Scouting",
    layout="wide",
    initial_sidebar_state="expanded",
)


@st.cache_resource
def get_driver():
    return GraphDatabase.driver(
        os.getenv("NEO4J_URI"),
        auth=(os.getenv("NEO4J_USER", "neo4j"), os.getenv("NEO4J_PASSWORD")),
    )


def run_query(cypher: str, **params):
    driver = get_driver()
    with driver.session() as s:
        return s.run(cypher, **params).data()


# ── Sidebar navigation ────────────────────────────────────────────────────────

st.sidebar.title("Problem Scouting")
st.sidebar.caption("Multi-agent disruption tracker")
st.sidebar.divider()

page = st.sidebar.radio(
    "Navigate",
    ["📚 BM Library", "🔀 Transition Case Studies", "📋 Input Review Queue",
     "📊 Graph Overview", "🧠 Hypothesis Review", "📈 Top Opportunities",
     "🔬 Validation Review", "📝 Editorial Queue", "🔄 Pipeline Monitor"],
)

st.sidebar.divider()
st.sidebar.markdown("""
**Build status**

| Part | Status |
|------|--------|
| 1–4 Infrastructure | ✅ |
| 5 Manual BM Entry | ✅ |
| 6 Company Upload | ✅ |
| 7 Technology Entry | ✅ |
| 8 Internet Scan | ✅ |
| 9 Input Review UI | ✅ |
| 10–13 Extraction | ✅ |
| 14–17 Analysis | ✅ |
| 18–21 Research | ✅ |
| 22–23 Monitor | ✅ |
| 24–25 Editorial | ✅ |
| 26–28 Orchestration | ✅ |
""")


# ── Page: Graph Overview ──────────────────────────────────────────────────────

# ── Page: BM Library ─────────────────────────────────────────────────────────

if page == "📚 BM Library":
    st.title("📚 Business Model Library")
    st.caption("Review, edit and annotate all business models. Every change is logged with your reason and used to calibrate extraction.")

    # ── Load all BMs ──────────────────────────────────────────────────────────
    bms = run_query("""
        MATCH (b:BusinessModel)
        OPTIONAL MATCH (b)<-[:FROM_BIM]-(v:TransformationVector)
        OPTIONAL MATCH (e:Evidence)-[:SUPPORTS]->(v)
        WITH b,
             count(DISTINCT v) AS vector_count,
             count(DISTINCT e) AS evidence_count
        RETURN b.bim_id        AS id,
               b.name          AS name,
               b.description   AS description,
               b.source        AS source,
               b.examples      AS examples,
               b.status        AS status,
               b.pending_human_review AS pending_review,
               b.added_by      AS added_by,
               b.created_at    AS created_at,
               vector_count,
               evidence_count
        ORDER BY b.bim_id
    """)

    # ── Filters ───────────────────────────────────────────────────────────────
    col_f1, col_f2, col_f3 = st.columns([3, 2, 2])
    with col_f1:
        search = st.text_input("🔍 Search", placeholder="Filter by name or description...")
    with col_f2:
        show_pending = st.checkbox("Show pending review only", value=False)
    with col_f3:
        sort_by = st.selectbox("Sort by", ["ID", "Name", "Evidence count", "Vector count"])

    # Filter
    filtered = bms
    if search:
        q = search.lower()
        filtered = [b for b in filtered if q in (b["name"] or "").lower()
                    or q in (b["description"] or "").lower()]
    if show_pending:
        filtered = [b for b in filtered if b.get("pending_review")]

    # Sort
    if sort_by == "Name":
        filtered = sorted(filtered, key=lambda x: x["name"] or "")
    elif sort_by == "Evidence count":
        filtered = sorted(filtered, key=lambda x: x["evidence_count"] or 0, reverse=True)
    elif sort_by == "Vector count":
        filtered = sorted(filtered, key=lambda x: x["vector_count"] or 0, reverse=True)

    st.divider()
    st.caption(f"Showing {len(filtered)} of {len(bms)} business models")

    # ── Change log loader ─────────────────────────────────────────────────────
    changelog_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "data", "bm_changelog.jsonl"
    )

    def append_changelog(entry: dict):
        os.makedirs(os.path.dirname(changelog_path), exist_ok=True)
        with open(changelog_path, "a") as f:
            f.write(json.dumps(entry) + "\n")

    def load_changelog(bim_id: str) -> list:
        if not os.path.exists(changelog_path):
            return []
        entries = []
        with open(changelog_path) as f:
            for line in f:
                try:
                    e = json.loads(line.strip())
                    if e.get("bim_id") == bim_id:
                        entries.append(e)
                except Exception:
                    pass
        return entries

    # ── Render each BM ────────────────────────────────────────────────────────
    for bm in filtered:
        bim_id  = bm["id"]
        name    = bm["name"] or bim_id
        pending = bm.get("pending_review")
        status  = bm.get("status", "Active") or "Active"

        label = f"{'🔴 ' if pending else ''}{bim_id}: {name}"
        with st.expander(label, expanded=False):

            # ── Top row: clickable stat buttons ───────────────────────────────
            vec_key = f"show_vectors_{bim_id}"
            evd_key = f"show_evidence_{bim_id}"
            if vec_key not in st.session_state:
                st.session_state[vec_key] = False
            if evd_key not in st.session_state:
                st.session_state[evd_key] = False

            m1, m2, m3, m4 = st.columns(4)
            with m1:
                vec_count = bm["vector_count"] or 0
                vec_label = f"🔗 {vec_count} Transitions FROM"
                if st.button(vec_label, key=f"btn_vec_{bim_id}",
                             help="Click to see all recorded transition paths from this model",
                             use_container_width=True):
                    st.session_state[vec_key] = not st.session_state[vec_key]
            with m2:
                evd_count = bm["evidence_count"] or 0
                evd_label = f"📄 {evd_count} Evidence nodes"
                if st.button(evd_label, key=f"btn_evd_{bim_id}",
                             help="Click to see all real-world examples linked to this model",
                             use_container_width=True):
                    st.session_state[evd_key] = not st.session_state[evd_key]
            with m3:
                st.metric("Status", status)
            with m4:
                st.metric("Added by", bm.get("added_by") or "system")

            # ── Vectors detail panel ──────────────────────────────────────────
            if st.session_state[vec_key]:
                st.markdown("---")
                st.markdown(f"### 🔗 All transitions FROM **{name}**")
                all_vectors = run_query("""
                    MATCH (v:TransformationVector)-[:FROM_BIM]->(f:BusinessModel {bim_id: $bid})
                    MATCH (v)-[:TO_BIM]->(t:BusinessModel)
                    OPTIONAL MATCH (h:DisruptionHypothesis)-[:GENERATED_FROM]->(v)
                    WITH v, t, h,
                         size([(e:Evidence)-[:SUPPORTS]->(v)|e]) AS ev_count
                    RETURN v.vector_id       AS vector_id,
                           t.name            AS to_name,
                           t.bim_id          AS to_id,
                           v.signal_strength AS signal,
                           v.opportunity_score AS opp_score,
                           ev_count,
                           h.hypothesis_id   AS hyp_id,
                           h.conviction_score AS conviction,
                           h.title           AS hyp_title
                    ORDER BY coalesce(v.signal_strength, 0) DESC
                """, bid=bim_id)

                if not all_vectors:
                    st.info("No transitions recorded from this model yet.")
                else:
                    st.caption(f"{len(all_vectors)} transition paths recorded")
                    for vrow in all_vectors:
                        sig   = vrow.get("signal") or 0
                        opp   = vrow.get("opp_score") or 0
                        ev_n  = vrow.get("ev_count") or 0
                        sig_bar = "█" * int(sig * 10) + "░" * (10 - int(sig * 10))

                        with st.container(border=True):
                            c1, c2, c3, c4 = st.columns([3, 2, 2, 2])
                            c1.markdown(f"**→ {vrow['to_name']}**")
                            c1.caption(f"`{vrow['vector_id']}`")
                            c2.markdown(f"Signal: `{sig:.3f}`")
                            c2.caption(sig_bar)
                            c3.markdown(f"Opportunity: `{opp:.3f}`")
                            c3.caption(f"{ev_n} evidence node{'s' if ev_n != 1 else ''}")
                            if vrow.get("hyp_id"):
                                c4.markdown(f"💡 Hypothesis")
                                c4.caption(f"Conviction: {vrow.get('conviction') or 0:.2f}")
                                if vrow.get("hyp_title"):
                                    st.caption(f"*\"{vrow['hyp_title'][:100]}\"*")
                            else:
                                c4.caption("No hypothesis yet")

                            # Show evidence quotes for this vector
                            ev_rows = run_query("""
                                MATCH (e:Evidence)-[:SUPPORTS]->(v:TransformationVector {vector_id: $vid})
                                RETURN e.evidence_quote       AS quote,
                                       e.transition_summary   AS summary,
                                       e.companies_mentioned  AS companies,
                                       e.source_url           AS url,
                                       e.source_type          AS stype,
                                       e.confidence           AS conf,
                                       e.created_at           AS created
                                ORDER BY e.confidence DESC
                            """, vid=vrow["vector_id"])
                            if ev_rows:
                                eq_key = f"show_eq_{vrow['vector_id']}"
                                if eq_key not in st.session_state:
                                    st.session_state[eq_key] = False
                                if st.button(f"📄 {len(ev_rows)} evidence quote{'s' if len(ev_rows)>1 else ''}",
                                             key=f"btn_eq_{vrow['vector_id']}"):
                                    st.session_state[eq_key] = not st.session_state[eq_key]
                                if st.session_state[eq_key]:
                                    for er in ev_rows:
                                        companies = er.get("companies") or []
                                        if isinstance(companies, str):
                                            try:
                                                import json as _json
                                                companies = _json.loads(companies)
                                            except Exception:
                                                companies = [companies]
                                        company_str = ", ".join(companies) if companies else "unknown company"
                                        st.markdown(f"**🏢 {company_str}**")
                                        summary = er.get("summary") or ""
                                        if summary:
                                            st.markdown(f"_{summary}_")
                                        quote = er.get("quote") or ""
                                        if quote and quote != summary:
                                            st.markdown(f"> \"{quote}\"")
                                        src = er.get("url") or ""
                                        if src:
                                            st.caption(f"conf={er.get('conf') or 0:.2f}  ·  [{src[:60]}]({src})")
                                        else:
                                            st.caption(f"conf={er.get('conf') or 0:.2f}")
                                        st.divider()

            # ── Evidence detail panel ─────────────────────────────────────────
            if st.session_state[evd_key]:
                st.markdown("---")
                st.markdown(f"### 📄 All evidence linked to **{name}**")
                all_evidence = run_query("""
                    MATCH (e:Evidence)-[:SUPPORTS]->(v:TransformationVector)-[:FROM_BIM]->(f:BusinessModel {bim_id: $bid})
                    MATCH (v)-[:TO_BIM]->(t:BusinessModel)
                    RETURN e.evidence_id          AS evd_id,
                           e.evidence_quote        AS quote,
                           e.transition_summary    AS summary,
                           e.companies_mentioned   AS companies,
                           e.source_url            AS url,
                           e.source_type           AS stype,
                           e.confidence            AS conf,
                           e.created_at            AS created,
                           t.name                  AS to_name,
                           v.vector_id             AS vector_id
                    ORDER BY e.confidence DESC
                """, bid=bim_id)

                if not all_evidence:
                    st.info("No evidence recorded yet for transitions from this model.")
                else:
                    st.caption(f"{len(all_evidence)} real-world examples found")
                    for er in all_evidence:
                        with st.container(border=True):
                            src = er.get("url") or ""
                            conf = er.get("conf") or 0
                            companies = er.get("companies") or []
                            if isinstance(companies, str):
                                try:
                                    import json as _json
                                    companies = _json.loads(companies)
                                except Exception:
                                    companies = [companies]
                            company_str = ", ".join(companies) if companies else "unknown company"

                            # Header row: company → destination
                            st.markdown(f"**🏢 {company_str}** &nbsp;→&nbsp; **{er.get('to_name', '?')}**")

                            # Summary (cleaner than raw quote)
                            summary = er.get("summary") or ""
                            if summary:
                                st.markdown(f"_{summary}_")

                            # Raw quote (secondary)
                            quote = er.get("quote") or ""
                            if quote and quote != summary:
                                st.markdown(f"> \"{quote}\"")

                            # Footer: confidence + source
                            footer_parts = [f"conf={conf:.2f}", er.get('stype') or ""]
                            if src:
                                st.caption(f"{' · '.join(p for p in footer_parts if p)}  🔗 [{src[:70]}]({src})")
                            else:
                                st.caption(" · ".join(p for p in footer_parts if p))

            st.divider()

            # ── Description ───────────────────────────────────────────────────
            st.markdown("**Description**")
            current_desc = bm["description"] or ""
            st.markdown(f"> {current_desc}" if current_desc else "_No description_")

            # ── Source + Examples ─────────────────────────────────────────────
            col_s, col_e = st.columns(2)
            with col_s:
                st.markdown("**Source**")
                st.caption(bm.get("source") or "_not set_")
            with col_e:
                st.markdown("**Company examples**")
                examples = bm.get("examples") or ""
                st.caption(examples if examples else "_none recorded_")

            st.divider()

            # ── Edit section ──────────────────────────────────────────────────
            with st.form(key=f"edit_{bim_id}"):
                st.markdown("**✏️ Edit this Business Model**")

                new_name = st.text_input("Name", value=name)
                new_desc = st.text_area("Description", value=current_desc, height=150,
                                        help="Full description used by the extraction LLM to classify companies")
                new_examples = st.text_input("Company examples (comma-separated)",
                                             value=examples,
                                             help="e.g. Netflix, Spotify, Adobe")
                new_source = st.text_input("Source / reference",
                                           value=bm.get("source") or "")
                change_reason = st.text_area(
                    "🧠 Why are you making this change?",
                    height=80,
                    placeholder="e.g. 'Description was too narrow — SaaS is being confused with generic subscription models. Adding clearer boundary around software delivery.'",
                    help="This is stored in the changelog and fed back to improve extraction prompts"
                )

                col_b1, col_b2 = st.columns([2, 1])
                with col_b1:
                    submitted = st.form_submit_button("💾 Save changes", type="primary")
                with col_b2:
                    mark_reviewed = st.form_submit_button("✅ Mark as reviewed")

                if submitted:
                    if not change_reason.strip():
                        st.error("Please provide a reason for the change — this is used to improve extraction.")
                    else:
                        now = datetime.now(timezone.utc).isoformat()
                        run_query("""
                            MATCH (b:BusinessModel {bim_id: $bid})
                            SET b.name        = $name,
                                b.description = $desc,
                                b.examples    = $examples,
                                b.source      = $source,
                                b.last_edited_at = $now,
                                b.last_edited_by = 'editorial'
                        """, bid=bim_id, name=new_name, desc=new_desc,
                             examples=new_examples, source=new_source, now=now)

                        append_changelog({
                            "bim_id":      bim_id,
                            "name":        name,
                            "timestamp":   now,
                            "editor":      "editorial",
                            "field":       "description+name+examples",
                            "old_desc":    current_desc,
                            "new_desc":    new_desc,
                            "old_name":    name,
                            "new_name":    new_name,
                            "reason":      change_reason.strip(),
                        })
                        st.success(f"✅ {bim_id} updated and change logged.")
                        st.rerun()

                if mark_reviewed:
                    run_query("""
                        MATCH (b:BusinessModel {bim_id: $bid})
                        SET b.pending_human_review = false,
                            b.reviewed_at = $now
                    """, bid=bim_id, now=datetime.now(timezone.utc).isoformat())
                    st.success("Marked as reviewed.")
                    st.rerun()

            # ── Change history ────────────────────────────────────────────────
            history = load_changelog(bim_id)
            if history:
                hist_key = f"show_hist_{bim_id}"
                if hist_key not in st.session_state:
                    st.session_state[hist_key] = False
                if st.button(f"📋 Change history ({len(history)} edit{'s' if len(history)>1 else ''})",
                             key=f"btn_hist_{bim_id}"):
                    st.session_state[hist_key] = not st.session_state[hist_key]
                if st.session_state[hist_key]:
                    for entry in reversed(history):
                        st.markdown(f"**{entry.get('timestamp','')[:19]}**")
                        st.markdown(f"*Reason:* {entry.get('reason','')}")
                        if entry.get('old_desc') != entry.get('new_desc'):
                            st.markdown(f"*Description changed*")
                        if entry.get('old_name') != entry.get('new_name'):
                            st.markdown(f"*Renamed:* `{entry.get('old_name')}` → `{entry.get('new_name')}`")
                        st.divider()

    # ── Global changelog export ───────────────────────────────────────────────
    st.divider()
    st.subheader("📋 Full Change Log")
    if os.path.exists(changelog_path):
        with open(changelog_path) as f:
            all_entries = [json.loads(l) for l in f if l.strip()]
        if all_entries:
            st.caption(f"{len(all_entries)} total edits logged")
            st.dataframe(
                [{"Time": e.get("timestamp","")[:19],
                  "BM": e.get("bim_id",""),
                  "Name": e.get("new_name",""),
                  "Reason": e.get("reason","")[:80]}
                 for e in reversed(all_entries)],
                use_container_width=True, hide_index=True
            )
            if st.button("📥 Download changelog as JSON"):
                st.download_button(
                    "Download", data=open(changelog_path).read(),
                    file_name="bm_changelog.jsonl", mime="application/json"
                )
    else:
        st.caption("No changes logged yet.")


# ── Page: Transition Case Studies ────────────────────────────────────────────

elif page == "🔀 Transition Case Studies":
    st.title("🔀 Transition Case Studies")
    st.caption("Every recorded real-world business model transition — the companies, the story, the causal forces. Edit any case and log your reason.")

    # ── Changelog helpers ─────────────────────────────────────────────────────
    tc_log_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "data", "transition_changelog.jsonl"
    )

    def tc_append(entry: dict):
        os.makedirs(os.path.dirname(tc_log_path), exist_ok=True)
        with open(tc_log_path, "a") as f:
            f.write(json.dumps(entry) + "\n")

    def tc_load(evidence_id: str) -> list:
        if not os.path.exists(tc_log_path):
            return []
        out = []
        with open(tc_log_path) as f:
            for line in f:
                try:
                    e = json.loads(line.strip())
                    if e.get("evidence_id") == evidence_id:
                        out.append(e)
                except Exception:
                    pass
        return out

    # ── Filters ───────────────────────────────────────────────────────────────
    col_f1, col_f2, col_f3, col_f4 = st.columns([3, 2, 2, 2])
    with col_f1:
        tc_search = st.text_input("🔍 Search companies or summary", key="tc_search",
                                   placeholder="e.g. Netflix, Adobe, Spotify...")
    with col_f2:
        # Get distinct from-BMs for filter
        from_bms = run_query("""
            MATCH (e:Evidence)-[:SUPPORTS]->(v:TransformationVector)-[:FROM_BIM]->(f:BusinessModel)
            RETURN DISTINCT f.name AS name ORDER BY f.name
        """)
        from_options = ["All"] + [r["name"] for r in from_bms]
        tc_from = st.selectbox("From BM", from_options, key="tc_from")
    with col_f3:
        to_bms = run_query("""
            MATCH (e:Evidence)-[:SUPPORTS]->(v:TransformationVector)-[:TO_BIM]->(t:BusinessModel)
            RETURN DISTINCT t.name AS name ORDER BY t.name
        """)
        to_options = ["All"] + [r["name"] for r in to_bms]
        tc_to = st.selectbox("To BM", to_options, key="tc_to")
    with col_f4:
        tc_sort = st.selectbox("Sort by", ["Signal strength", "Confidence", "Newest first"],
                                key="tc_sort")

    # ── Load evidence nodes (one per company story) ───────────────────────────
    sort_clause = {
        "Signal strength": "ORDER BY v.signal_strength DESC, e.confidence DESC",
        "Confidence":      "ORDER BY e.confidence DESC",
        "Newest first":    "ORDER BY e.extracted_at DESC",
    }[tc_sort]

    from_filter = "" if tc_from == "All" else "AND f.name = $from_bm"
    to_filter   = "" if tc_to   == "All" else "AND t.name = $to_bm"

    cases = run_query(f"""
        MATCH (e:Evidence)-[:SUPPORTS]->(v:TransformationVector)-[:FROM_BIM]->(f:BusinessModel)
        MATCH (v)-[:TO_BIM]->(t:BusinessModel)
        WHERE 1=1 {from_filter} {to_filter}
        OPTIONAL MATCH (h:DisruptionHypothesis)-[:GENERATED_FROM]->(v)
        WITH e, v, f, t, h,
             size([(sc:Scalar)<-[:IMPACTS]-(v)|sc]) AS scalar_count
        RETURN e.evidence_id          AS eid,
               e.companies_mentioned  AS companies,
               e.transition_summary   AS summary,
               e.evidence_quote       AS quote,
               e.source_url           AS url,
               e.source_type          AS stype,
               e.confidence           AS conf,
               e.extracted_at         AS extracted_at,
               v.vector_id            AS vid,
               v.signal_strength      AS signal,
               v.opportunity_score    AS opp,
               f.name                 AS from_bm,
               f.bim_id               AS from_id,
               t.name                 AS to_bm,
               t.bim_id               AS to_id,
               scalar_count,
               h.hypothesis_id        AS hyp_id,
               h.title                AS hyp_title,
               h.conviction_score     AS conviction,
               h.thesis               AS thesis,
               h.counter_argument     AS counter,
               h.disruption_type      AS dtype,
               h.time_horizon         AS horizon,
               h.ai_disruption_link   AS ai_link
        {sort_clause}
    """, from_bm=tc_from, to_bm=tc_to)

    # Apply text search
    if tc_search:
        q = tc_search.lower()
        cases = [c for c in cases if
                 q in " ".join(c.get("companies") or []).lower()
                 or q in (c.get("summary") or "").lower()
                 or q in (c.get("quote") or "").lower()
                 or q in (c.get("from_bm") or "").lower()
                 or q in (c.get("to_bm") or "").lower()]

    st.divider()
    st.caption(f"{len(cases)} case studies")

    if not cases:
        st.info("No case studies match your filters.")
    else:
        DIRECTION_OPTIONS = ["increases", "neutral", "decreases"]
        STRENGTH_OPTIONS  = ["strong", "moderate", "weak"]
        IMPACT_SCORE_MAP  = {
            ("increases", "strong"):   2,
            ("increases", "moderate"): 1,
            ("neutral",   "strong"):   0,
            ("neutral",   "moderate"): 0,
            ("neutral",   "weak"):     0,
            ("decreases", "moderate"): -1,
            ("decreases", "strong"):   -2,
        }

        for case in cases:
            eid     = case.get("eid") or ""
            vid     = case.get("vid") or ""
            signal  = case.get("signal") or 0
            opp     = case.get("opp") or 0
            conf    = case.get("conf") or 0
            has_hyp = bool(case.get("hyp_id"))

            companies = case.get("companies") or []
            if isinstance(companies, str):
                try:    companies = json.loads(companies)
                except Exception: companies = [companies]
            company_str = ", ".join(companies) if companies else "Unknown company"

            transition_label = f"{case['from_bm']} → {case['to_bm']}"
            card_label = f"{transition_label}  ·  {company_str}"
            with st.expander(card_label, expanded=False):

                st.markdown(f"<u>**{transition_label}**</u>  —  {company_str}",
                            unsafe_allow_html=True)

                # ── Stat row ──────────────────────────────────────────────────
                s1, s2, s3, s4 = st.columns(4)
                s1.metric("Signal", f"{signal:.3f}")
                s2.metric("Confidence", f"{conf:.2f}")
                s3.metric("Scalars", case.get("scalar_count") or 0)
                s4.metric("Hypothesis", "✅ Yes" if has_hyp else "—")

                st.divider()

                # ── The Story ─────────────────────────────────────────────────
                st.markdown("#### 📖 The Story")
                summary = case.get("summary") or ""
                st.markdown(summary if summary else "_No summary available_")

                quote = case.get("quote") or ""
                if quote and quote.strip() != summary.strip():
                    st.markdown(f"> \"{quote}\"")

                src = case.get("url") or ""
                if src:
                    st.caption(f"🔗 Source: [{src[:80]}]({src})  ·  {case.get('stype') or ''}")

                st.divider()

                # ── Edit story toggle ──────────────────────────────────────────
                ev_edit_key = f"show_ev_edit_{eid}"
                if ev_edit_key not in st.session_state:
                    st.session_state[ev_edit_key] = False
                if st.button("✏️ Edit story", key=f"btn_ev_edit_{eid}"):
                    st.session_state[ev_edit_key] = not st.session_state[ev_edit_key]

                if st.session_state[ev_edit_key]:
                    with st.form(key=f"tc_edit_{eid}"):
                        new_companies = st.text_input(
                            "Companies involved (comma-separated)", value=", ".join(companies)
                        )
                        new_summary = st.text_area(
                            "Transition story / summary", value=summary, height=100
                        )
                        flag_wrong = st.checkbox("🚩 Flag as incorrect transition (wrong From/To BM)")
                        correct_from = correct_to = ""
                        if flag_wrong:
                            correct_from = st.text_input("Correct 'From' BM", placeholder="e.g. Licensing")
                            correct_to   = st.text_input("Correct 'To' BM",   placeholder="e.g. SaaS")
                        edit_reason = st.text_area(
                            "🧠 Why are you making this change?", height=70,
                            placeholder="e.g. 'PayPal moved to Platform/Marketplace, not pure Subscription.'",
                        )
                        tc_submitted = st.form_submit_button("💾 Save", type="primary")
                        if tc_submitted:
                            if not edit_reason.strip():
                                st.error("Please explain the reason.")
                            else:
                                now = datetime.now(timezone.utc).isoformat()
                                new_co_list = [c.strip() for c in new_companies.split(",") if c.strip()]
                                run_query("""
                                    MATCH (e:Evidence {evidence_id: $eid})
                                    SET e.companies_mentioned = $companies,
                                        e.transition_summary  = $summary,
                                        e.last_edited_at      = $now,
                                        e.last_edited_by      = 'editorial'
                                """, eid=eid, companies=new_co_list, summary=new_summary, now=now)
                                tc_append({
                                    "evidence_id":   eid,
                                    "vector_id":     vid,
                                    "from_bm":       case["from_bm"],
                                    "to_bm":         case["to_bm"],
                                    "timestamp":     now,
                                    "change_type":   "evidence_edit",
                                    "old_companies": companies,
                                    "new_companies": new_co_list,
                                    "old_summary":   summary,
                                    "new_summary":   new_summary,
                                    "flagged_wrong": flag_wrong,
                                    "correct_from":  correct_from,
                                    "correct_to":    correct_to,
                                    "reason":        edit_reason.strip(),
                                })
                                flag_msg = f" ⚠️ Flagged ({correct_from} → {correct_to})" if flag_wrong else ""
                                st.success(f"✅ Updated.{flag_msg}")
                                st.session_state[ev_edit_key] = False
                                st.rerun()

                # Edit history for this evidence node
                ev_history = tc_load(eid)
                if ev_history:
                    for h in reversed(ev_history):
                        flag_note = " 🚩" if h.get("flagged_wrong") else ""
                        st.caption(f"_Edited {h.get('timestamp','')[:16]}{flag_note}: {h.get('reason','')[:100]}_")

                # ── Scalars ───────────────────────────────────────────────────
                scalars = run_query("""
                    MATCH (v:TransformationVector {vector_id: $vid})-[r:IMPACTS]->(sc:Scalar)
                    RETURN sc.scalar_id       AS scalar_id,
                           sc.name            AS name,
                           r.direction        AS direction,
                           r.impact_strength  AS strength,
                           r.impact_score     AS score,
                           r.rationale        AS rationale
                    ORDER BY r.impact_score DESC
                """, vid=vid)

                if scalars:
                    st.markdown("#### ⚙️ Causal Forces (Scalars)")
                    st.caption("The conditions pushing / pulling this transition")
                    for sc in scalars:
                        score    = sc.get("score") or 0
                        arrow    = "⬆️" if sc.get("direction") == "increases" else "⬇️"
                        strength = sc.get("strength") or "—"
                        col_sc1, col_sc2 = st.columns([3, 1])
                        with col_sc1:
                            st.markdown(f"{arrow} **{sc['name'][:70]}**")
                            rationale = sc.get("rationale") or ""
                            if rationale:
                                st.caption(rationale[:300])
                        with col_sc2:
                            color = "green" if score and score > 0 else "red"
                            st.markdown(f":{color}[**{strength}** ({'+' if score and score > 0 else ''}{score})]")

                    # Scalar edit toggle (keyed on vid — shared across evidence nodes for same vector)
                    sc_edit_key = f"show_sc_edit_{vid}"
                    if sc_edit_key not in st.session_state:
                        st.session_state[sc_edit_key] = False
                    # Use vid+eid to give each card its own button key
                    if st.button("✏️ Edit scalars", key=f"btn_sc_edit_{vid}_{eid}"):
                        st.session_state[sc_edit_key] = not st.session_state[sc_edit_key]

                    if st.session_state[sc_edit_key]:
                        with st.form(key=f"sc_edit_form_{vid}_{eid}"):
                            st.markdown("**Edit scalar directions and rationale:**")
                            sc_edits = []
                            for sc in scalars:
                                st.markdown(f"**{sc['name'][:80]}**")
                                ec1, ec2 = st.columns(2)
                                cur_dir = sc.get("direction") or "increases"
                                cur_str = sc.get("strength") or "moderate"
                                new_dir = ec1.selectbox(
                                    "Direction", DIRECTION_OPTIONS,
                                    index=DIRECTION_OPTIONS.index(cur_dir) if cur_dir in DIRECTION_OPTIONS else 0,
                                    key=f"sc_dir_{vid}_{eid}_{sc['scalar_id']}",
                                )
                                new_str = ec2.selectbox(
                                    "Strength", STRENGTH_OPTIONS,
                                    index=STRENGTH_OPTIONS.index(cur_str) if cur_str in STRENGTH_OPTIONS else 1,
                                    key=f"sc_str_{vid}_{eid}_{sc['scalar_id']}",
                                )
                                new_rat = st.text_area(
                                    "Rationale", value=sc.get("rationale") or "",
                                    height=70, key=f"sc_rat_{vid}_{eid}_{sc['scalar_id']}",
                                )
                                sc_edits.append({
                                    "scalar_id": sc["scalar_id"],
                                    "new_dir": new_dir,
                                    "new_str": new_str,
                                    "new_rat": new_rat,
                                })
                                st.divider()
                            sc_reason = st.text_area(
                                "🧠 Why are you correcting these scalars?", height=70,
                                placeholder="e.g. 'The marginal cost direction was inverted.'",
                            )
                            sc_submitted = st.form_submit_button("💾 Save scalar edits", type="primary")
                            if sc_submitted:
                                if not sc_reason.strip():
                                    st.error("Please explain your reason.")
                                else:
                                    now = datetime.now(timezone.utc).isoformat()
                                    for ed in sc_edits:
                                        new_score = IMPACT_SCORE_MAP.get((ed["new_dir"], ed["new_str"]), 0)
                                        run_query("""
                                            MATCH (v:TransformationVector {vector_id: $vid})
                                                  -[r:IMPACTS]->(sc:Scalar {scalar_id: $sid})
                                            SET r.direction       = $dir,
                                                r.impact_strength = $strength,
                                                r.impact_score    = $score,
                                                r.rationale       = $rationale,
                                                r.edited_at       = $now,
                                                r.edited_by       = 'editorial'
                                        """, vid=vid, sid=ed["scalar_id"],
                                            dir=ed["new_dir"], strength=ed["new_str"],
                                            score=new_score, rationale=ed["new_rat"], now=now)
                                    tc_append({
                                        "evidence_id":  eid,
                                        "vector_id":    vid,
                                        "from_bm":      case["from_bm"],
                                        "to_bm":        case["to_bm"],
                                        "timestamp":    now,
                                        "change_type":  "scalar_edit",
                                        "scalar_edits": [{"scalar_id": ed["scalar_id"],
                                                          "direction": ed["new_dir"],
                                                          "strength":  ed["new_str"]} for ed in sc_edits],
                                        "reason": sc_reason.strip(),
                                    })
                                    st.success(f"✅ {len(sc_edits)} scalar(s) updated.")
                                    st.session_state[sc_edit_key] = False
                                    st.rerun()

                # ── Hypothesis ────────────────────────────────────────────────
                if has_hyp:
                    st.markdown("#### 💡 Disruption Hypothesis")
                    hcol1, hcol2, hcol3 = st.columns(3)
                    hcol1.metric("Conviction", f"{case.get('conviction') or 0:.2f}")
                    hcol2.metric("Type", case.get("dtype") or "—")
                    hcol3.metric("Horizon", case.get("horizon") or "—")
                    if case.get("hyp_title"):
                        st.markdown(f"**{case['hyp_title']}**")
                    thesis = case.get("thesis") or ""
                    if thesis:
                        st.markdown(thesis)
                    counter = case.get("counter") or ""
                    if counter:
                        st.markdown("**⚠️ Counter-argument:**")
                        st.markdown(f"_{counter}_")
                    ai_link = case.get("ai_link") or ""
                    if ai_link:
                        st.markdown(f"**🤖 AI link:** {ai_link}")

    # ── Global transition changelog ───────────────────────────────────────────
    st.divider()
    st.subheader("📋 Transition Edit Log")
    if os.path.exists(tc_log_path):
        with open(tc_log_path) as f:
            all_tc = [json.loads(l) for l in f if l.strip()]
        if all_tc:
            st.caption(f"{len(all_tc)} total edits · {sum(1 for e in all_tc if e.get('flagged_wrong'))} flagged as wrong")
            st.dataframe(
                [{"Time":     e.get("timestamp","")[:19],
                  "From→To":  f"{e.get('from_bm','')} → {e.get('to_bm','')}",
                  "Companies": ", ".join(e.get("new_companies") or [])[:40],
                  "Flagged":  "🚩" if e.get("flagged_wrong") else "",
                  "Reason":   e.get("reason","")[:80]}
                 for e in reversed(all_tc)],
                use_container_width=True, hide_index=True
            )
    else:
        st.caption("No edits logged yet.")


# ── Page: Graph Overview ──────────────────────────────────────────────────────

if page == "📊 Graph Overview":
    st.title("Graph Overview")

    counts = run_query("""
        MATCH (n)
        RETURN labels(n)[0] AS label, count(n) AS cnt
        ORDER BY cnt DESC
    """)

    rel_counts = run_query("""
        MATCH ()-[r]->()
        RETURN type(r) AS rel_type, count(r) AS cnt
        ORDER BY cnt DESC
    """)

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Node counts")
        for row in counts:
            st.metric(row["label"], row["cnt"])

    with col2:
        st.subheader("Relationship counts")
        for row in rel_counts:
            st.metric(row["rel_type"], row["cnt"])


# ── Page: Input Review Queue ──────────────────────────────────────────────────

elif page == "📋 Input Review Queue":
    st.title("Input Review Queue")
    st.caption("Items added by agents that need human approval before entering the active graph.")

    # Tabs for each node type
    tab_bm, tab_co, tab_tech = st.tabs(["Business Models", "Companies", "Technologies"])

    # ── Business Models ────────────────────────────────────────────────────────
    with tab_bm:
        bms = run_query("""
            MATCH (n:BusinessModel)
            WHERE n.pending_human_review = true
            RETURN n.bim_id AS id, n.name AS name, n.source AS source,
                   n.description AS description,
                   n.typical_margins AS margins,
                   coalesce(n.confidence, 0.0) AS confidence,
                   n.created_at AS created_at
            ORDER BY n.created_at DESC
        """)

        if not bms:
            st.success("No business models pending review.")
        else:
            st.info(f"{len(bms)} business model(s) pending review")
            for bm in bms:
                with st.expander(f"**{bm['id']}** — {bm['name']}  ·  conf={bm['confidence']:.2f}  ·  source={bm['source']}"):
                    col1, col2 = st.columns([3, 1])
                    with col1:
                        st.markdown(f"**Description:** {bm['description'] or '—'}")
                        st.markdown(f"**Typical margins:** {bm['margins'] or '—'}")
                        st.caption(f"Created: {bm['created_at']}")

                    with col2:
                        approve_key = f"approve_bm_{bm['id']}"
                        reject_key  = f"reject_bm_{bm['id']}"

                        if st.button("✅ Approve", key=approve_key):
                            run_query("""
                                MATCH (n:BusinessModel {bim_id: $id})
                                SET n.pending_human_review = false,
                                    n.reviewed_at = $now,
                                    n.reviewed_by = 'human_ui'
                            """, id=bm["id"],
                                now=datetime.now(timezone.utc).isoformat())
                            st.success(f"{bm['id']} approved")
                            st.rerun()

                        if st.button("❌ Reject", key=reject_key):
                            run_query("""
                                MATCH (n:BusinessModel {bim_id: $id})
                                SET n.status = 'Rejected',
                                    n.pending_human_review = false,
                                    n.reviewed_at = $now,
                                    n.reviewed_by = 'human_ui'
                            """, id=bm["id"],
                                now=datetime.now(timezone.utc).isoformat())
                            st.warning(f"{bm['id']} rejected")
                            st.rerun()

    # ── Companies ──────────────────────────────────────────────────────────────
    with tab_co:
        companies = run_query("""
            MATCH (n:Company)
            WHERE n.pending_human_review = true
            OPTIONAL MATCH (n)-[r:CURRENTLY_USES]->(bm:BusinessModel)
            RETURN n.company_id AS id, n.name AS name,
                   n.hq_country AS country,
                   n.funding_stage AS stage,
                   n.ai_involvement AS ai,
                   coalesce(n.bm_confidence, 0.0) AS bm_conf,
                   bm.bim_id AS bim_id, bm.name AS bm_name,
                   n.description AS description,
                   n.bm_rationale AS rationale,
                   n.created_at AS created_at
            ORDER BY n.created_at DESC
        """)

        if not companies:
            st.success("No companies pending review.")
        else:
            st.info(f"{len(companies)} company(s) pending review")
            for co in companies:
                with st.expander(
                    f"**{co['id']}** — {co['name']}  ·  "
                    f"{co['bim_id'] or '?'} {co['bm_name'] or ''}  ·  "
                    f"conf={co['bm_conf']:.2f}  ·  AI={co['ai'] or '?'}"
                ):
                    col1, col2 = st.columns([3, 1])
                    with col1:
                        st.markdown(f"**Description:** {co['description'] or '—'}")
                        st.markdown(f"**BM Rationale:** {co['rationale'] or '—'}")
                        st.markdown(
                            f"**Stage:** {co['stage'] or '?'}  |  "
                            f"**Country:** {co['country'] or '?'}  |  "
                            f"**AI involvement:** {co['ai'] or '?'}"
                        )
                        st.caption(f"Created: {co['created_at']}")

                    with col2:
                        approve_key = f"approve_co_{co['id']}"
                        reject_key  = f"reject_co_{co['id']}"

                        if st.button("✅ Approve", key=approve_key):
                            run_query("""
                                MATCH (n:Company {company_id: $id})
                                SET n.pending_human_review = false,
                                    n.reviewed_at = $now,
                                    n.reviewed_by = 'human_ui'
                            """, id=co["id"],
                                now=datetime.now(timezone.utc).isoformat())
                            st.success(f"{co['id']} approved")
                            st.rerun()

                        if st.button("❌ Reject", key=reject_key):
                            run_query("""
                                MATCH (n:Company {company_id: $id})
                                SET n.pending_human_review = false,
                                    n.status = 'Rejected',
                                    n.reviewed_at = $now,
                                    n.reviewed_by = 'human_ui'
                            """, id=co["id"],
                                now=datetime.now(timezone.utc).isoformat())
                            st.warning(f"{co['id']} rejected")
                            st.rerun()

    # ── Technologies ───────────────────────────────────────────────────────────
    with tab_tech:
        techs = run_query("""
            MATCH (n:Technology)
            WHERE n.pending_human_review = true
            OPTIONAL MATCH (n)-[r:INFLUENCES]->(sc:Scalar)
            WITH n, count(sc) AS scalar_count
            RETURN n.tech_id AS id, n.name AS name, n.short_name AS short_name,
                   n.category AS category,
                   n.maturity_level AS maturity,
                   n.maturity_source AS maturity_source,
                   coalesce(n.confidence, 0.0) AS confidence,
                   n.description AS description,
                   n.disruption_thesis AS thesis,
                   scalar_count,
                   n.created_at AS created_at
            ORDER BY n.created_at DESC
        """)

        if not techs:
            st.success("No technologies pending review.")
        else:
            st.info(f"{len(techs)} technology(s) pending review")
            for tech in techs:
                with st.expander(
                    f"**{tech['id']}** — {tech['name']}  ·  "
                    f"maturity={tech['maturity']}/100  ·  "
                    f"{tech['scalar_count']} scalar impacts"
                ):
                    col1, col2 = st.columns([3, 1])
                    with col1:
                        st.markdown(f"**Description:** {tech['description'] or '—'}")
                        st.markdown(f"**Disruption thesis:** {tech['thesis'] or '—'}")
                        st.markdown(
                            f"**Maturity:** {tech['maturity']}/100  |  "
                            f"**Source:** {tech['maturity_source'] or '?'}  |  "
                            f"**Scalar impacts:** {tech['scalar_count']}"
                        )
                        st.caption(f"Created: {tech['created_at']}")

                    with col2:
                        approve_key = f"approve_tech_{tech['id']}"
                        reject_key  = f"reject_tech_{tech['id']}"

                        if st.button("✅ Approve", key=approve_key):
                            run_query("""
                                MATCH (n:Technology {tech_id: $id})
                                SET n.pending_human_review = false,
                                    n.reviewed_at = $now,
                                    n.reviewed_by = 'human_ui'
                            """, id=tech["id"],
                                now=datetime.now(timezone.utc).isoformat())
                            st.success(f"{tech['id']} approved")
                            st.rerun()

                        if st.button("❌ Reject", key=reject_key):
                            run_query("""
                                MATCH (n:Technology {tech_id: $id})
                                SET n.pending_human_review = false,
                                    n.tracking_status = 'Rejected',
                                    n.reviewed_at = $now,
                                    n.reviewed_by = 'human_ui'
                            """, id=tech["id"],
                                now=datetime.now(timezone.utc).isoformat())
                            st.warning(f"{tech['id']} rejected")
                            st.rerun()


# ── Page: Hypothesis Review ───────────────────────────────────────────────────

elif page == "🧠 Hypothesis Review":
    st.title("Hypothesis Review")
    st.caption("Disruption hypotheses generated from extraction pipeline. Review, approve, or escalate.")

    sort_by = st.selectbox("Sort by", ["conviction_score", "evidence_count", "created_at"],
                           index=0)

    hypotheses = run_query(f"""
        MATCH (h:DisruptionHypothesis)
        OPTIONAL MATCH (h)-[:GENERATED_FROM]->(v:TransformationVector)
        OPTIONAL MATCH (f:BusinessModel {{bim_id: h.from_bim_id}})
        OPTIONAL MATCH (t:BusinessModel {{bim_id: h.to_bim_id}})
        RETURN h.hypothesis_id       AS hid,
               h.title               AS title,
               h.thesis              AS thesis,
               h.counter_argument    AS counter,
               h.conviction_score    AS conviction,
               h.disruption_type     AS dtype,
               h.time_horizon        AS horizon,
               h.ai_technology_link  AS ai_link,
               h.primary_scalar_driver AS primary_scalar,
               h.companies_exposed   AS companies,
               h.evidence_count      AS evidence_count,
               h.status              AS status,
               h.pending_human_review AS pending,
               f.name AS from_name, t.name AS to_name,
               v.signal_strength     AS signal,
               v.opportunity_score   AS opp_score,
               h.created_at          AS created_at
        ORDER BY h.{sort_by} DESC
    """)

    if not hypotheses:
        st.info("No hypotheses yet. Run the extraction pipeline to generate some.")
    else:
        st.info(f"{len(hypotheses)} hypothesis(es) in graph")

        for h in hypotheses:
            conv = h.get("conviction") or 0
            conv_color = "🟢" if conv >= 0.7 else ("🟡" if conv >= 0.5 else "🔴")
            pending_badge = "🔔 Pending review" if h.get("pending") else ""
            status = h.get("status", "")

            with st.expander(
                f"{conv_color} **{h['hid']}** — {h['title'] or 'Untitled'}  "
                f"·  conviction={conv:.2f}  ·  {h.get('dtype','?')}  "
                f"·  {h.get('horizon','?')}  {pending_badge}"
            ):
                col1, col2 = st.columns([3, 1])

                with col1:
                    st.markdown(f"**{h.get('from_name','?')} → {h.get('to_name','?')}**")
                    st.markdown(f"**Thesis:**\n{h.get('thesis','—')}")
                    st.markdown("---")
                    st.markdown(f"**Counter-argument:** *{h.get('counter','—')}*")
                    st.markdown("---")

                    metrics_cols = st.columns(4)
                    metrics_cols[0].metric("Conviction", f"{conv:.2f}")
                    metrics_cols[1].metric("Signal", f"{h.get('signal') or 0:.3f}")
                    metrics_cols[2].metric("Opp Score", f"{h.get('opp_score') or 0:.4f}")
                    metrics_cols[3].metric("Evidence", str(h.get("evidence_count") or 0))

                    st.markdown(
                        f"**AI link:** {h.get('ai_link') or '—'}  |  "
                        f"**Primary scalar:** {h.get('primary_scalar') or '—'}  |  "
                        f"**Companies exposed:** {', '.join(h.get('companies') or []) or '—'}"
                    )
                    st.caption(f"Created: {h.get('created_at','')} · Status: {status}")

                with col2:
                    if h.get("pending"):
                        if st.button("✅ Approve", key=f"hyp_approve_{h['hid']}"):
                            run_query("""
                                MATCH (n:DisruptionHypothesis {hypothesis_id: $id})
                                SET n.pending_human_review = false,
                                    n.status = 'Validated',
                                    n.reviewed_at = $now,
                                    n.reviewed_by = 'human_ui'
                            """, id=h["hid"],
                                now=datetime.now(timezone.utc).isoformat())
                            st.success("Approved → Validated")
                            st.rerun()

                        if st.button("⬆️ Escalate", key=f"hyp_escalate_{h['hid']}"):
                            run_query("""
                                MATCH (n:DisruptionHypothesis {hypothesis_id: $id})
                                SET n.status = 'Escalated',
                                    n.escalated_at = $now
                            """, id=h["hid"],
                                now=datetime.now(timezone.utc).isoformat())
                            st.warning("Escalated for deep research")
                            st.rerun()

                        if st.button("❌ Reject", key=f"hyp_reject_{h['hid']}"):
                            run_query("""
                                MATCH (n:DisruptionHypothesis {hypothesis_id: $id})
                                SET n.pending_human_review = false,
                                    n.status = 'Rejected',
                                    n.reviewed_at = $now,
                                    n.reviewed_by = 'human_ui'
                            """, id=h["hid"],
                                now=datetime.now(timezone.utc).isoformat())
                            st.error("Rejected")
                            st.rerun()


# ── Page: Top Opportunities ───────────────────────────────────────────────────

elif page == "📈 Top Opportunities":
    st.title("Top Opportunities")
    st.caption("TransformationVectors ranked by composite opportunity score.")

    col1, col2 = st.columns([1, 3])
    with col1:
        top_n = st.slider("Show top N", 5, 50, 20)
        min_tech = st.number_input("Min tech score", 0, 15, 0)

    opportunities = run_query(f"""
        MATCH (v:TransformationVector)
        WHERE v.opportunity_score IS NOT NULL
        MATCH (v)-[:FROM_BIM]->(f:BusinessModel)
        MATCH (v)-[:TO_BIM]->(t:BusinessModel)
        OPTIONAL MATCH (h:DisruptionHypothesis)-[:GENERATED_FROM]->(v)
        WHERE coalesce(v.best_tech_score, 0) >= {min_tech}
        RETURN v.vector_id        AS vid,
               f.name             AS from_name,
               t.name             AS to_name,
               v.opportunity_score AS opp,
               v.signal_strength  AS signal,
               v.best_tech_score  AS tech,
               v.best_conviction  AS conviction,
               v.scalar_alignment AS alignment,
               h.title            AS hyp_title,
               v.tech_score_kggen AS kggen,
               v.tech_score_gnns  AS gnns,
               v.tech_score_synthetic AS synth
        ORDER BY v.opportunity_score DESC
        LIMIT {top_n}
    """)

    if not opportunities:
        st.info("No ranked opportunities yet. Run `python analysis/opportunity_ranker.py` first.")
    else:
        for i, o in enumerate(opportunities, 1):
            opp = o.get("opp") or 0
            color = "🟢" if opp >= 0.30 else ("🟡" if opp >= 0.15 else "⚪")
            with st.expander(
                f"{color} #{i} **{o['from_name']} → {o['to_name']}**  "
                f"·  score={opp:.4f}"
            ):
                cols = st.columns(5)
                cols[0].metric("Opp Score", f"{opp:.4f}")
                cols[1].metric("Signal", f"{o.get('signal') or 0:.3f}")
                cols[2].metric("Best Tech", str(o.get("tech") or 0))
                cols[3].metric("Conviction", f"{o.get('conviction') or 0:.2f}")
                cols[4].metric("Scalar Align", f"{o.get('alignment') or 0:.2f}")

                st.markdown(
                    f"**Tech scores:** GNNs={o.get('gnns') or 0}  "
                    f"KGGen={o.get('kggen') or 0}  Synthetic={o.get('synth') or 0}"
                )
                if o.get("hyp_title"):
                    st.markdown(f"**Hypothesis:** {o['hyp_title']}")
                st.caption(o["vid"])


# ── Page: Validation Review ───────────────────────────────────────────────────

elif page == "🔬 Validation Review":
    st.title("Validation Review")
    st.caption("Research briefs and adversarial counter-briefs for each hypothesis.")

    validated = run_query("""
        MATCH (h:DisruptionHypothesis)
        OPTIONAL MATCH (ev:Evaluation)-[:EVALUATES]->(h)
        WITH h, collect({type: ev.evaluation_type, conf: ev.confidence}) AS evals
        RETURN h.hypothesis_id    AS hid,
               h.title            AS title,
               h.thesis           AS thesis,
               h.status           AS status,
               h.conviction_score AS conviction,
               h.validation_score AS validation,
               h.research_confidence AS research_conf,
               h.counter_confidence  AS counter_conf,
               h.companies_actively_transitioning AS transitioning,
               h.from_bim_id AS from_id, h.to_bim_id AS to_id,
               evals
        ORDER BY coalesce(h.validation_score, h.conviction_score) DESC
    """)

    if not validated:
        st.info("No hypotheses yet. Run the extraction pipeline.")
    else:
        for h in validated:
            evals = h.get("evals") or []
            has_research = any(e.get("type") == "deep_research" for e in evals)
            has_counter  = any(e.get("type") == "counter_research" for e in evals)
            val = h.get("validation") or h.get("conviction") or 0
            status = h.get("status", "Hypothesis")
            status_icon = {"Validated": "🟢", "Contested": "🔴",
                           "Hypothesis": "🟡", "Escalated": "⬆️"}.get(status, "⚪")

            with st.expander(
                f"{status_icon} **{h['hid']}** — {h['title'] or 'Untitled'}  "
                f"·  val={val:.2f}  ·  {status}  "
                f"·  {'✅research' if has_research else '❌research'}  "
                f"·  {'✅counter' if has_counter else '❌counter'}"
            ):
                tab_brief, tab_counter, tab_score = st.tabs(
                    ["Research Brief", "Counter Brief", "Validation Score"]
                )

                with tab_brief:
                    if has_research:
                        brief_ev = next((e for e in evals if e.get("type") == "deep_research"), {})
                        st.metric("Research confidence", f"{brief_ev.get('conf', 0):.2f}")
                        driver = get_driver()
                        with driver.session() as s:
                            ev_rec = s.run("""
                                MATCH (ev:Evaluation {evaluation_id:$eid})
                                RETURN ev.content_json AS cj
                            """, eid=f"EVAL_{h['hid']}_DEEP_RESEARCH").single()
                        driver.close()
                        if ev_rec:
                            try:
                                content = json.loads(ev_rec["cj"])
                                st.markdown(f"**Summary:** {content.get('research_summary','')}")
                                st.markdown("**Supporting:**")
                                for e in content.get("supporting_evidence", [])[:4]:
                                    st.markdown(f"- [{e.get('strength','')}] {e.get('claim','')}")
                                st.markdown("**Refuting:**")
                                for e in content.get("refuting_evidence", [])[:3]:
                                    st.markdown(f"- [{e.get('strength','')}] {e.get('claim','')}")
                            except Exception:
                                pass
                    else:
                        st.info("No research brief yet.")
                        if st.button(f"Run deep research", key=f"run_research_{h['hid']}"):
                            from research.deep_researcher import research_hypothesis
                            research_hypothesis(h["hid"])
                            st.rerun()

                with tab_counter:
                    if has_counter:
                        driver = get_driver()
                        with driver.session() as s:
                            ev_rec = s.run("""
                                MATCH (ev:Evaluation {evaluation_id:$eid})
                                RETURN ev.content_json AS cj, ev.confidence AS conf
                            """, eid=f"EVAL_{h['hid']}_COUNTER_RESEARCH").single()
                        driver.close()
                        if ev_rec:
                            st.metric("Adversarial confidence", f"{ev_rec['conf']:.2f}")
                            try:
                                content = json.loads(ev_rec["cj"])
                                st.markdown(f"**Counter thesis:** {content.get('counter_thesis','')}")
                                st.markdown("**Structural barriers:**")
                                for b in content.get("structural_barriers", [])[:4]:
                                    st.markdown(f"- [{b.get('severity','')}] **{b.get('barrier','')}**: {b.get('description','')}")
                            except Exception:
                                pass
                    else:
                        st.info("No counter brief yet.")
                        if st.button(f"Run counter research", key=f"run_counter_{h['hid']}"):
                            from research.deep_researcher import counter_research_hypothesis
                            counter_research_hypothesis(h["hid"])
                            st.rerun()

                with tab_score:
                    cols = st.columns(3)
                    cols[0].metric("Conviction", f"{h.get('conviction') or 0:.2f}")
                    cols[1].metric("Research", f"{h.get('research_conf') or 0:.2f}")
                    cols[2].metric("Validation", f"{val:.2f}")
                    if st.button("Recalculate validation score", key=f"rescore_{h['hid']}"):
                        from research.validation_scorer import score_hypothesis
                        score_hypothesis(h["hid"])
                        st.rerun()


# ── Page: Editorial Queue ─────────────────────────────────────────────────────

elif page == "📝 Editorial Queue":
    st.title("Editorial Queue")
    st.caption(
        "Hypotheses ranked by editorial priority. Add notes, change status, "
        "or promote to deep research."
    )

    priority_filter = st.selectbox(
        "Filter by staleness",
        ["All", "URGENT (no score)", "STALE (new evidence)", "DRIFT (signal changed)", "CURRENT"],
    )

    queue_rows = run_query("""
        MATCH (h:DisruptionHypothesis)
        OPTIONAL MATCH (h)-[:GENERATED_FROM]->(v:TransformationVector)
        OPTIONAL MATCH (ev:Evaluation)-[:EVALUATES]->(h)
        WITH h, v, count(DISTINCT ev) AS eval_count
        RETURN h.hypothesis_id        AS hid,
               h.title                AS title,
               h.status               AS status,
               h.conviction_score     AS conviction,
               h.validation_score     AS validation,
               h.research_confidence  AS research_conf,
               h.counter_confidence   AS counter_conf,
               h.researched_at        AS researched_at,
               h.validated_at         AS validated_at,
               h.signal_at_validation AS signal_at_val,
               h.editorial_note       AS editorial_note,
               h.editorial_priority   AS editorial_priority,
               v.signal_strength      AS signal,
               eval_count
        ORDER BY
            CASE WHEN h.validation_score IS NULL THEN 0 ELSE 1 END ASC,
            h.conviction_score DESC
    """)

    if not queue_rows:
        st.info("No hypotheses in graph yet.")
    else:
        # Apply staleness filter (client-side for simplicity)
        def classify_staleness(row):
            if row.get("validation") is None:
                return "URGENT (no score)"
            researched = row.get("researched_at")
            # We don't have latest_evidence_at in this query; classify remaining as CURRENT
            sig_now  = row.get("signal") or 0
            sig_val  = row.get("signal_at_val") or 0
            if sig_val > 0 and abs(sig_now - sig_val) > 0.10:
                return "DRIFT (signal changed)"
            return "CURRENT"

        if priority_filter != "All":
            queue_rows = [r for r in queue_rows if classify_staleness(r) == priority_filter]

        st.info(f"{len(queue_rows)} hypothesis(es) shown")

        for h in queue_rows:
            staleness = classify_staleness(h)
            stale_icon = {
                "URGENT (no score)":     "🔴",
                "STALE (new evidence)":  "🟡",
                "DRIFT (signal changed)": "🔵",
                "CURRENT":               "🟢",
            }.get(staleness, "⚪")

            val_str = f"{h['validation']:.4f}" if h.get("validation") is not None else "unscored"
            with st.expander(
                f"{stale_icon} **{h['hid']}** — {h['title'] or 'Untitled'}  "
                f"·  {staleness}  ·  conviction={h.get('conviction') or 0:.2f}  "
                f"·  val={val_str}  ·  evals={h.get('eval_count',0)}"
            ):
                col1, col2 = st.columns([3, 1])

                with col1:
                    metrics_cols = st.columns(4)
                    metrics_cols[0].metric("Conviction", f"{h.get('conviction') or 0:.2f}")
                    metrics_cols[1].metric("Signal",     f"{h.get('signal') or 0:.4f}")
                    metrics_cols[2].metric("Research",   f"{h.get('research_conf') or 0:.2f}")
                    metrics_cols[3].metric("Validation", val_str)

                    # Editorial note
                    current_note = h.get("editorial_note") or ""
                    st.markdown("**Editorial note:**")
                    note_key = f"note_{h['hid']}"
                    new_note = st.text_area(
                        "Add/edit note",
                        value=current_note,
                        key=note_key,
                        height=80,
                        label_visibility="collapsed",
                    )

                    note_col1, note_col2 = st.columns([1, 3])
                    if note_col1.button("💾 Save note", key=f"save_note_{h['hid']}"):
                        run_query("""
                            MATCH (n:DisruptionHypothesis {hypothesis_id: $id})
                            SET n.editorial_note = $note,
                                n.editorial_updated_at = $now,
                                n.editorial_updated_by = 'human_ui'
                        """, id=h["hid"], note=new_note,
                            now=datetime.now(timezone.utc).isoformat())
                        st.success("Note saved")
                        st.rerun()

                    # Priority label
                    priorities = ["—", "high", "medium", "low", "watch", "archive"]
                    current_pri = h.get("editorial_priority") or "—"
                    current_idx = priorities.index(current_pri) if current_pri in priorities else 0
                    new_pri = st.selectbox(
                        "Editorial priority",
                        priorities,
                        index=current_idx,
                        key=f"pri_{h['hid']}",
                    )
                    if new_pri != current_pri:
                        run_query("""
                            MATCH (n:DisruptionHypothesis {hypothesis_id: $id})
                            SET n.editorial_priority = $pri
                        """, id=h["hid"], pri=new_pri if new_pri != "—" else None)

                with col2:
                    st.markdown("**Actions**")

                    # Trigger deep research
                    if st.button("🔬 Research now", key=f"ed_research_{h['hid']}"):
                        from research.deep_researcher import (
                            research_hypothesis, counter_research_hypothesis
                        )
                        with st.spinner("Running research..."):
                            research_hypothesis(h["hid"])
                            counter_research_hypothesis(h["hid"])
                        from research.validation_scorer import score_hypothesis
                        score_hypothesis(h["hid"])
                        st.success("Research + scoring complete")
                        st.rerun()

                    # Status change
                    st.markdown("**Set status:**")
                    for new_status, icon in [
                        ("Validated", "✅"), ("Contested", "❌"), ("Hypothesis", "🔄"),
                    ]:
                        if h.get("status") != new_status:
                            if st.button(
                                f"{icon} {new_status}", key=f"ed_set_{h['hid']}_{new_status}"
                            ):
                                run_query("""
                                    MATCH (n:DisruptionHypothesis {hypothesis_id: $id})
                                    SET n.status = $status,
                                        n.editorial_status_set_at = $now,
                                        n.editorial_status_set_by = 'human_ui'
                                """, id=h["hid"], status=new_status,
                                    now=datetime.now(timezone.utc).isoformat())
                                st.success(f"Status set to {new_status}")
                                st.rerun()


# ── Page: Pipeline Monitor ────────────────────────────────────────────────────

elif page == "🔄 Pipeline Monitor":
    st.title("Pipeline Monitor")
    st.caption("Run pipeline stages and monitor system health.")

    col_left, col_right = st.columns([2, 1])

    with col_right:
        st.subheader("Quick Actions")
        dry_run_flag = st.checkbox("Dry run (no writes)", value=True)

        if st.button("🔄 Run analysis loop", help="aggregate + trends + rank + monitor + score"):
            from orchestrator.pipeline import run_pipeline
            with st.spinner("Running analysis pipeline..."):
                result = run_pipeline(
                    stages=["aggregate", "trends", "rank", "monitor", "score"],
                    dry_run=dry_run_flag,
                )
            if result["success"]:
                st.success(f"Pipeline complete — {len(result['stages_run'])} stages")
            else:
                st.error(f"Errors: {list(result['errors'].keys())}")
            st.json(result["results"])

        if st.button("📊 Health check"):
            from orchestrator.pipeline import stage_health
            with st.spinner("Checking graph health..."):
                health = stage_health()
            st.json(health)

        if st.button("🔍 Staleness report"):
            from evaluation.monitor import run_monitor
            with st.spinner("Running monitor..."):
                report = run_monitor(drift_threshold=0.10, dry_run=True)
            st.metric("URGENT", report["urgent"])
            st.metric("STALE",  report["stale"])
            st.metric("DRIFT",  report["drift"])
            st.metric("CURRENT", report["current"])
            if report["queue"]:
                st.dataframe([{
                    "ID":       q["hypothesis_id"],
                    "Staleness": q["staleness"],
                    "Reason":   q["staleness_reason"],
                    "Val":      q.get("validation_score"),
                } for q in report["queue"]])

        st.divider()
        if st.button("📸 Take snapshot", help="Save a JSON snapshot of current graph state"):
            try:
                from core.snapshot import take_snapshot
                with st.spinner("Taking snapshot..."):
                    snap = take_snapshot(label="manual")
                if "error" in snap:
                    st.error(snap["error"])
                else:
                    total_nodes = sum(snap.get("node_counts", {}).values())
                    st.success(f"Snapshot saved — {total_nodes} nodes")
            except Exception as e:
                st.error(str(e))

    with col_left:
        tab_stats, tab_errors, tab_history = st.tabs(["📊 Stats", "🔴 Errors", "🕓 History"])

        with tab_stats:
            st.subheader("System Stats")
            stats = run_query("""
                MATCH (n)
                RETURN labels(n)[0] AS label, count(n) AS cnt
                ORDER BY cnt DESC
            """)
            if stats:
                import pandas as pd
                df = pd.DataFrame(stats)
                st.dataframe(df, use_container_width=True)

            st.subheader("Hypothesis Pipeline Status")
            hyp_stats = run_query("""
                MATCH (h:DisruptionHypothesis)
                OPTIONAL MATCH (ev:Evaluation)-[:EVALUATES]->(h)
                WITH h, count(DISTINCT ev) AS eval_count
                RETURN
                    h.status AS status,
                    count(h) AS total,
                    count(h.validation_score) AS scored,
                    count(h.research_confidence) AS researched,
                    sum(eval_count) AS total_evals
            """)
            if hyp_stats:
                st.dataframe(hyp_stats, use_container_width=True)

            st.subheader("Top 5 Opportunities")
            top_opps = run_query("""
                MATCH (v:TransformationVector)
                WHERE v.opportunity_score IS NOT NULL
                MATCH (v)-[:FROM_BIM]->(f:BusinessModel)
                MATCH (v)-[:TO_BIM]->(t:BusinessModel)
                RETURN f.name AS from_name, t.name AS to_name,
                       round(v.opportunity_score, 4) AS opp_score,
                       round(v.signal_strength, 4) AS signal
                ORDER BY v.opportunity_score DESC
                LIMIT 5
            """)
            if top_opps:
                st.dataframe(top_opps, use_container_width=True)

        with tab_errors:
            try:
                from core.error_log import read_recent, clear_log, error_count_last_24h
                err_24h = error_count_last_24h()
                if err_24h > 0:
                    st.error(f"🔴 {err_24h} error(s) in the last 24 hours")
                else:
                    st.success("✅ No errors in the last 24 hours")

                col_e1, col_e2 = st.columns([3, 1])
                with col_e2:
                    if st.button("🗑 Clear log", help="Truncate the error log file"):
                        clear_log()
                        st.success("Log cleared")
                        st.rerun()

                errors_raw = read_recent(50)
                if not errors_raw:
                    st.info("No errors recorded yet.")
                else:
                    import pandas as pd
                    err_df = pd.DataFrame([{
                        "Time":     r.get("timestamp", "")[:19].replace("T", " "),
                        "Module":   r.get("module", ""),
                        "Function": r.get("function", ""),
                        "Error":    f"{r.get('error_type','')}: {r.get('message','')[:80]}",
                        "Context":  str(r.get("context", "")),
                    } for r in errors_raw])
                    st.dataframe(err_df, use_container_width=True)

                    with st.expander("🔍 Full traceback (last error)"):
                        if errors_raw:
                            st.code(errors_raw[0].get("traceback", ""), language="python")
            except ImportError:
                st.warning("core.error_log not available — run `git init` and restart.")

        with tab_history:
            st.subheader("Code Commits")
            try:
                from core.version_control import get_recent_commits, git_restore, current_sha
                sha_now = current_sha()
                st.caption(f"Current HEAD: `{sha_now}`")

                commits = get_recent_commits(15)
                if not commits:
                    st.info("No commits yet. Run the pipeline once to auto-commit.")
                else:
                    for c in commits:
                        c_col1, c_col2 = st.columns([4, 1])
                        with c_col1:
                            st.markdown(f"**`{c['sha']}`** {c['message']}")
                            st.caption(f"{c['timestamp']} — {c['author']}")
                        with c_col2:
                            if c["sha"] != sha_now:
                                restore_key = f"restore_{c['sha']}"
                                if st.button("⏪ Restore", key=restore_key,
                                             help=f"Restore *.py files to {c['sha']}"):
                                    res = git_restore(c["full_sha"])
                                    if res["restored"]:
                                        st.success(f"Restored to {res['sha']}")
                                    else:
                                        st.error(res["error"])
                        st.divider()
            except ImportError:
                st.warning("core.version_control not available — install GitPython.")

            st.subheader("Data Snapshots")
            try:
                from core.snapshot import list_snapshots
                snapshots = list_snapshots(10)
                if not snapshots:
                    st.info("No snapshots yet. Click '📸 Take snapshot' or run the pipeline.")
                else:
                    for snap in snapshots:
                        with st.expander(
                            f"🗃 {snap['timestamp'][:16]}  ·  {snap['label'] or 'snapshot'}  "
                            f"·  {snap['total_nodes']} nodes"
                        ):
                            s_col1, s_col2, s_col3 = st.columns(3)
                            s_col1.metric("Hypotheses", snap["hypotheses"])
                            s_col2.metric("Vectors", snap["vectors"])
                            s_col3.metric("Evidence", snap["evidence"])
                            st.json(snap["full_data"])
            except ImportError:
                st.warning("core.snapshot not available.")
